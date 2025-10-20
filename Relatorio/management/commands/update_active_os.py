import requests
import time
import pytz
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from tqdm import tqdm

from django.core.management.base import BaseCommand
from django.conf import settings
from django.db.models import Q
from Relatorio.models import OrdemDeServico, Tarefa


class Command(BaseCommand):
    help = 'Atualiza as OS com status "Em Processo" ou "Em Verificação" buscando os dados mais recentes na API.'

    TOKEN_URL = "https://app.fracttal.com/oauth/token"
    BASE_URL = "https://app.fracttal.com/api"
    MAX_WORKERS = 15

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Armazenamento seguro para o token em ambiente com threads
        self.token_storage = {'token': None, 'lock': threading.Lock()}

    def handle(self, *args, **kwargs):
        self.stdout.write(self.style.SUCCESS("=" * 60))
        self.stdout.write(self.style.SUCCESS("🚀 INICIANDO ATUALIZAÇÃO DE OS ATIVAS 🚀"))
        self.stdout.write(self.style.SUCCESS("=" * 60))

        try:
            self._obter_token_acesso()  # Obtém o token inicial

            # 1. Busca no banco de dados local as OS que precisam de verificação
            os_para_verificar = OrdemDeServico.objects.filter(
                Q(Status='Em Processo') | Q(Status='Em Verificação')
            )

            lista_os_folios = list(os_para_verificar.values_list('OS', flat=True))
            total_a_verificar = len(lista_os_folios)

            if total_a_verificar == 0:
                self.stdout.write(self.style.SUCCESS("✅ Nenhuma OS ativa para verificar. Tudo em dia!"))
                return

            self.stdout.write(f"🔍 Encontradas {total_a_verificar} OS ativas para sincronizar.")

            # 2. Executa a busca paralela na API
            resultados = self._executar_busca_paralela(lista_os_folios)

            # 3. Processa os resultados e atualiza o banco
            self._processar_resultados_finais(resultados)

            self.stdout.write(self.style.SUCCESS("\n" + "=" * 60))
            self.stdout.write(self.style.SUCCESS("✅ PROCESSO DE ATUALIZAÇÃO FINALIZADO!"))
            self.stdout.write(self.style.SUCCESS("=" * 60))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"\n❌ ERRO CRÍTICO DURANTE A EXECUÇÃO: {e}"))

    def _obter_token_acesso(self):
        with self.token_storage['lock']:
            self.stdout.write("🔑 Obtendo/Renovando token de acesso...")
            try:
                auth = (settings.FRACTTAL_CLIENT_ID, settings.FRACTTAL_CLIENT_SECRET)
                data = {"grant_type": "client_credentials", "scope": "api"}
                response = requests.post(self.TOKEN_URL, auth=auth, data=data)
                response.raise_for_status()
                self.token_storage['token'] = response.json()["access_token"]
                self.stdout.write(self.style.SUCCESS("✅ Token obtido com sucesso!"))
            except requests.exceptions.RequestException as e:
                raise Exception(f"Erro crítico ao obter token: {e}")

    def _fetch_os_data(self, wo_folio):
        """Busca os dados de uma única OS na API."""
        url = f"{self.BASE_URL}/work_orders/{wo_folio}"
        headers = {"Authorization": f"Bearer {self.token_storage['token']}"}

        try:
            response = requests.get(url, headers=headers, timeout=20)

            if response.status_code == 401:  # Token expirado
                self._obter_token_acesso()
                # Tenta novamente com o novo token
                headers = {"Authorization": f"Bearer {self.token_storage['token']}"}
                response = requests.get(url, headers=headers, timeout=20)

            if response.status_code == 404:
                return {'status': '404', 'wo_folio': wo_folio}

            response.raise_for_status()
            return {'status': 'SUCCESS', 'wo_folio': wo_folio, 'data': response.json()}

        except requests.exceptions.RequestException as e:
            return {'status': 'ERROR', 'wo_folio': wo_folio, 'error': str(e)}

    def _executar_busca_paralela(self, lista_os_folios):
        resultados = []
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = {executor.submit(self._fetch_os_data, folio): folio for folio in lista_os_folios}

            for future in tqdm(as_completed(futures), total=len(lista_os_folios), desc="Atualizando OS"):
                resultados.append(future.result())
        return resultados

    def _processar_resultados_finais(self, resultados):
        atualizadas, nao_encontradas, com_erro = 0, 0, 0
        for res in resultados:
            if res['status'] == 'SUCCESS':
                # Os dados de uma OS podem vir em múltiplos itens de tarefa
                for item in res['data'].get('data', []):
                    self._atualizar_db_com_item(item)
                atualizadas += 1
            elif res['status'] == '404':
                # A OS não existe mais na Fracttal, então cancelamos localmente
                OrdemDeServico.objects.filter(OS=res['wo_folio']).update(Status='Cancelado')
                nao_encontradas += 1
            else:
                self.stdout.write(self.style.WARNING(f"\nFalha ao buscar {res['wo_folio']}: {res.get('error')}"))
                com_erro += 1

        self.stdout.write(self.style.SUCCESS(f"\nResumo da Sincronização:"))
        self.stdout.write(f"  - {atualizadas} OS atualizadas com sucesso.")
        self.stdout.write(f"  - {nao_encontradas} OS não encontradas na API (marcadas como 'Cancelado').")
        self.stdout.write(f"  - {com_erro} OS falharam durante a busca.")

    def _atualizar_db_com_item(self, item):
        """Usa a lógica de update_or_create para um item de tarefa da API."""
        wo_folio = item.get('wo_folio')
        id_tarefa_api = item.get('id_work_orders_tasks')
        if not wo_folio or not id_tarefa_api:
            return

        # --- Processa todas as datas, incluindo as novas ---
        data_criacao = self._parse_e_converter_datetime(item.get("creation_date"))
        data_finalizacao = self._parse_e_converter_datetime(item.get("wo_final_date"))
        data_inicio = self._parse_e_converter_datetime(item.get("initial_date"))
        data_verificacao = self._parse_e_converter_datetime(item.get("review_date"))
        data_programada = self._parse_e_converter_datetime(item.get("date_maintenance"))
        id_request = item.get("id_request")

        dados_os = {
            'Status': self._converter_status(item.get("id_status_work_order")),
            'Nivel_de_Criticidade': self._converter_criticidade(item.get("id_priorities")),
            'Criado_Por': item.get("created_by"),
            'Avanco_da_OS': item.get("completed_percentage"),
            'Ticket_ID': id_request,
            'Possui_Ticket': "Sim" if id_request is not None else "Não",
            'Local_Empresa': item.get("parent_description"),
            'Observacao_OS': item.get("task_note"),

            # Data de Criação
            'Data_Criacao_OS': data_criacao,
            'Ano_Criacao': data_criacao.year if data_criacao else None,
            'Mes_Criacao': data_criacao.month if data_criacao else None,
            'Dia_Criacao': data_criacao.day if data_criacao else None,
            'Hora_Criacao': data_criacao.time() if data_criacao else None,

            # Data de Finalização
            'Data_Finalizacao_OS': data_finalizacao,
            'Ano_Finalizacao': data_finalizacao.year if data_finalizacao else None,
            'Mes_Finalizacao': data_finalizacao.month if data_finalizacao else None,
            'Dia_Finalizacao': data_finalizacao.day if data_finalizacao else None,
            'Hora_Finalizacao': data_finalizacao.time() if data_finalizacao else None,

            # Data de Início
            'Data_Iniciou_OS': data_inicio,
            'Ano_Inicio': data_inicio.year if data_inicio else None,
            'Mes_Inicio': data_inicio.month if data_inicio else None,
            'Dia_Inicio': data_inicio.day if data_inicio else None,
            'Hora_Inicio': data_inicio.time() if data_inicio else None,

            # --- NOVOS CAMPOS DE DATA ---
            'Data_Enviado_Verificacao': data_verificacao,
            'Data_Programada': data_programada,
        }
        os_obj, _ = OrdemDeServico.objects.update_or_create(OS=wo_folio, defaults=dados_os)

        dados_tarefa = {
            'ordem_de_servico': os_obj,
            'Ativo': item.get("items_log_description"),
            'Responsavel': item.get("personnel_description"),
            'Plano_de_Tarefas': item.get("description"),
            'Tipo_de_Tarefa': item.get("tasks_log_task_type_main"),
            'Duracao_Minutos': self._segundos_para_minutos(item.get("real_duration")),
            'Status_da_Tarefa': item.get("task_status"),
        }
        Tarefa.objects.update_or_create(id_tarefa_api=id_tarefa_api, defaults=dados_tarefa)

    # --- Funções de ajuda (Helpers) ---
    def _parse_e_converter_datetime(self, date_string):
        if not date_string: return None
        try:
            utc_time = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
            return utc_time.astimezone(pytz.timezone("America/Sao_Paulo"))
        except (ValueError, TypeError):
            return None

    def _converter_status(self, id_status):
        return {1: "Em Processo", 2: "Em Verificação", 3: "Concluído", 4: "Cancelado"}.get(id_status, "Desconhecido")

    def _converter_criticidade(self, id_prioridade):
        return {1: "Muito Alto", 2: "Alto", 3: "Médio", 4: "Baixo", 5: "Muito Baixo"}.get(id_prioridade, "Não definida")

    def _segundos_para_minutos(self, segundos):
        if segundos is None: return None
        try:
            return round(float(segundos) / 60, 2)
        except (ValueError, TypeError):
            return None