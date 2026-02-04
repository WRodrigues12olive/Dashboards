import requests
import time
import pytz
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from tqdm import tqdm

from django.core.management.base import BaseCommand
from django.conf import settings
from django.db.models import Max
from django.db.models.functions import Cast, Substr
from django.db.models import IntegerField

from Relatorio.models import OrdemDeServico, Tarefa
# Importação das regras de negócio
from Relatorio.mappings import (
    get_grupo_local, 
    get_local_detalhado, 
    get_grupo_tecnico, 
    get_grupo_tipo_tarefa,
    get_trt_specific_name
)

class Command(BaseCommand):
    help = 'Sincronização HISTÓRICA: Varre de OS1 até a última OS encontrada, atualizando tudo via busca individual.'

    TOKEN_URL = "https://app.fracttal.com/oauth/token"
    BASE_URL = "https://app.fracttal.com/api"
    MAX_WORKERS = 15  # Quantidade de requisições simultâneas

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.token_storage = {'token': None, 'lock': threading.Lock()}

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("=" * 80))
        self.stdout.write(self.style.SUCCESS(" INICIANDO VARREDURA COMPLETA (OS 1 -> FIM) 🚀"))
        self.stdout.write(self.style.WARNING(" Nota: Este processo busca cada OS individualmente. Pode levar alguns minutos."))
        self.stdout.write(self.style.SUCCESS("=" * 80))

        try:
            self._obter_token_acesso()

            # 1. Descobrir qual é o número da última OS no banco para saber até onde ir
            self.stdout.write(" Calculando o intervalo de busca...")
            
            # Pega o maior número de OS atual no banco (Ex: OS2050 -> 2050)
            max_os_qs = OrdemDeServico.objects.annotate(
                num_os=Cast(Substr('OS', 3), output_field=IntegerField())
            ).aggregate(Max('num_os'))
            
            ultimo_numero_banco = max_os_qs.get('num_os__max') or 0
            
            # Margem de segurança: Vai até a última do banco + 50 novas (para garantir que pegou as recentes)
            limite_busca = ultimo_numero_banco + 50
            
            if limite_busca < 100: 
                limite_busca = 100 # Mínimo para começar se o banco estiver vazio

            self.stdout.write(f" -> Última OS no banco: {ultimo_numero_banco}")
            self.stdout.write(f" -> Meta de Varredura: OS 1 até OS {limite_busca}")

            # Cria a lista de números para buscar: [1, 2, 3, ..., 2050, ..., 2100]
            lista_numeros = list(range(1, limite_busca + 1))

            # 2. Executa a busca massiva em paralelo
            resultados = self._executar_busca_paralela(lista_numeros)

            # 3. Processa e Salva
            self._processar_resultados_finais(resultados)

            self.stdout.write(self.style.SUCCESS("\n" + "=" * 80))
            self.stdout.write(self.style.SUCCESS(" SINCRONIZAÇÃO HISTÓRICA FINALIZADA!"))
            self.stdout.write(self.style.SUCCESS("=" * 80))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"\n❌ ERRO CRÍTICO: {e}"))

    def _obter_token_acesso(self):
        with self.token_storage['lock']:
            self.stdout.write("Obtendo/Renovando token de acesso...")
            try:
                auth = (settings.FRACTTAL_CLIENT_ID, settings.FRACTTAL_CLIENT_SECRET)
                data = {"grant_type": "client_credentials", "scope": "api"}
                response = requests.post(self.TOKEN_URL, auth=auth, data=data)
                response.raise_for_status()
                self.token_storage['token'] = response.json()["access_token"]
                self.stdout.write(self.style.SUCCESS("Token obtido!"))
            except requests.exceptions.RequestException as e:
                raise Exception(f"Erro ao obter token: {e}")

    def _fetch_os_data(self, os_number):
        """Busca uma OS específica (Ex: OS100)"""
        wo_folio = f"OS{os_number}"
        url = f"{self.BASE_URL}/work_orders/{wo_folio}"
        headers = {"Authorization": f"Bearer {self.token_storage['token']}"}

        try:
            response = requests.get(url, headers=headers, timeout=20)

            # Se der erro 401 (Token Expirado), renova e tenta de novo
            if response.status_code == 401:
                self._obter_token_acesso()
                headers = {"Authorization": f"Bearer {self.token_storage['token']}"}
                response = requests.get(url, headers=headers, timeout=20)

            if response.status_code == 404:
                return {'status': '404', 'os_number': os_number}

            response.raise_for_status()
            return {'status': 'SUCCESS', 'os_number': os_number, 'data': response.json()}

        except requests.exceptions.RequestException as e:
            return {'status': 'ERROR', 'os_number': os_number, 'error': str(e)}

    def _executar_busca_paralela(self, lista_numeros):
        resultados = []
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            # Cria as tarefas futuras
            futures = {executor.submit(self._fetch_os_data, num): num for num in lista_numeros}
            
            # Barra de progresso
            for future in tqdm(as_completed(futures), total=len(lista_numeros), desc="Baixando Histórico"):
                resultados.append(future.result())
        return resultados

    def _processar_resultados_finais(self, resultados):
        sucesso, nao_existem, erros = 0, 0, 0
        
        self.stdout.write("\nProcessando dados baixados e salvando no banco...")
        
        for res in resultados:
            if res['status'] == 'SUCCESS':
                api_data = res.get('data', {})
                lista_tarefas = api_data.get('data', [])

                if lista_tarefas:
                    for item in lista_tarefas:
                        self._atualizar_db_com_item(item)
                    sucesso += 1
                else:
                    # OS existe mas não tem tarefas (raro, mas acontece)
                    # Podemos optar por não fazer nada ou criar a OS sem tarefas
                    pass

            elif res['status'] == '404':
                nao_existem += 1
            else:
                erros += 1

        self.stdout.write(self.style.SUCCESS(f"\nResumo:"))
        self.stdout.write(f"  - {sucesso} OSs baixadas e atualizadas (Com Custo e Descrições).")
        self.stdout.write(f"  - {nao_existem} números de OS não existem na API (vazios).")
        self.stdout.write(f"  - {erros} erros de conexão.")

    def _atualizar_db_com_item(self, item):
        """Salva no banco (Update or Create) com todos os campos novos."""
        wo_folio = item.get('wo_folio')
        id_tarefa_api = item.get('id_work_orders_tasks')
        
        if not wo_folio or not id_tarefa_api: return

        # Datas
        data_criacao = self._parse_date(item.get("creation_date"))
        data_finalizacao = self._parse_date(item.get("wo_final_date"))
        data_inicio = self._parse_date(item.get("initial_date"))
        data_verificacao = self._parse_date(item.get("review_date"))
        data_programada = self._parse_date(item.get("date_maintenance"))
        id_request = item.get("id_request")

        # Regras de Local
        local_raw = item.get("parent_description")
        ativo_raw = item.get("items_log_description")
        
        grupo_local = get_grupo_local(local_raw)
        local_detalhado = get_local_detalhado(local_raw)

        if ativo_raw and 'HOSPITAL DE CLINICAS' in ativo_raw.upper():
            grupo_local = 'Hospital De Clinicas'
            local_detalhado = 'Hospital De Clinicas'
        elif ativo_raw and ('TRT' in ativo_raw.upper() or '4 REGIAO' in ativo_raw.upper()):
            if grupo_local != 'TRT' or local_detalhado == 'TRT Outros':
                novo = get_trt_specific_name(ativo_raw)
                if novo != 'TRT Outros':
                    grupo_local = 'TRT'
                    local_detalhado = novo

        # Dados OS
        dados_os = {
            'Status': self._converter_status(item.get("id_status_work_order")),
            'Nivel_de_Criticidade': self._converter_criticidade(item.get("id_priorities")),
            'Criado_Por': item.get("created_by"), 'Avanco_da_OS': item.get("completed_percentage"),
            'Ticket_ID': id_request, 'Possui_Ticket': "Sim" if id_request else "Não",
            'Local_Empresa': local_raw, 'Observacao_OS': item.get("task_note"),
            'Local_Agrupado': grupo_local, 'Local_Detalhado': local_detalhado,

            'Data_Criacao_OS': data_criacao,
            'Ano_Criacao': data_criacao.year if data_criacao else None,
            'Mes_Criacao': data_criacao.month if data_criacao else None,
            'Dia_Criacao': data_criacao.day if data_criacao else None,
            'Hora_Criacao': data_criacao.time() if data_criacao else None,

            'Data_Finalizacao_OS': data_finalizacao,
            'Ano_Finalizacao': data_finalizacao.year if data_finalizacao else None,
            'Mes_Finalizacao': data_finalizacao.month if data_finalizacao else None,
            'Dia_Finalizacao': data_finalizacao.day if data_finalizacao else None,
            'Hora_Finalizacao': data_finalizacao.time() if data_finalizacao else None,

            'Data_Iniciou_OS': data_inicio, 'Ano_Inicio': data_inicio.year if data_inicio else None,
            'Mes_Inicio': data_inicio.month if data_inicio else None,
            'Dia_Inicio': data_inicio.day if data_inicio else None,
            'Hora_Inicio': data_inicio.time() if data_inicio else None,

            'Data_Enviado_Verificacao': data_verificacao, 'Data_Programada': data_programada,
        }
        os_obj, _ = OrdemDeServico.objects.update_or_create(OS=wo_folio, defaults=dados_os)

        # Dados Tarefa (COM NOVOS CAMPOS DE CUSTO E DESCRIÇÃO)
        resp_raw = item.get("personnel_description")
        tipo_raw = item.get("tasks_log_task_type_main")

        dados_tarefa = {
            'ordem_de_servico': os_obj, 'Ativo': ativo_raw,
            'Responsavel': resp_raw, 'Plano_de_Tarefas': item.get("description"),
            'Tipo_de_Tarefa': tipo_raw,
            'Duracao_Minutos': self._segundos_para_minutos(item.get("real_duration")),
            'Status_da_Tarefa': item.get("task_status"),

            'types_description': item.get("types_description"),
            'causes_description': item.get("causes_description"),
            'detection_method_description': item.get("detection_method_description"),

            'Responsavel_Agrupado': get_grupo_tecnico(resp_raw),
            'Tipo_Tarefa_Agrupado': get_grupo_tipo_tarefa(tipo_raw),
        }
        Tarefa.objects.update_or_create(id_tarefa_api=id_tarefa_api, defaults=dados_tarefa)

    def _parse_date(self, d):
        if not d: return None
        try:
            return datetime.fromisoformat(d.replace('Z', '+00:00')).astimezone(pytz.timezone("America/Sao_Paulo"))
        except: return None

    def _converter_status(self, i):
        return {1: "Em Processo", 2: "Em Verificação", 3: "Concluído", 4: "Cancelado"}.get(i, "Desconhecido")

    def _converter_criticidade(self, i):
        return {1: "Muito Alto", 2: "Alto", 3: "Médio", 4: "Baixo", 5: "Muito Baixo"}.get(i, "Não definida")

    def _segundos_para_minutos(self, s):
        try: return round(float(s) / 60, 2)
        except: return None