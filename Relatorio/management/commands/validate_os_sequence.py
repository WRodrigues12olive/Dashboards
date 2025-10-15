import requests
import time
import pytz
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from tqdm import tqdm

from django.core.management.base import BaseCommand
from django.conf import settings
from Relatorio.models import OrdemDeServico, Tarefa


class Command(BaseCommand):
    help = 'Verifica a sequência de OS no banco de dados, encontra lacunas e busca os dados faltantes na API.'

    TOKEN_URL = "https://app.fracttal.com/oauth/token"
    BASE_URL = "https://app.fracttal.com/api"
    MAX_WORKERS = 15

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.token_storage = {'token': None, 'lock': threading.Lock()}

    def handle(self, *args, **kwargs):
        self.stdout.write(self.style.SUCCESS("=" * 60))
        self.stdout.write(self.style.SUCCESS("🚀 INICIANDO VALIDAÇÃO DE INTEGRIDADE DA SEQUÊNCIA DE OS 🚀"))
        self.stdout.write(self.style.SUCCESS("=" * 60))

        try:
            self._obter_token_acesso()

            self.stdout.write("🔍 Analisando o banco de dados local para encontrar lacunas...")
            all_os_folios = OrdemDeServico.objects.values_list('OS', flat=True)

            existing_numbers = set()
            max_number = 0
            for folio in all_os_folios:
                try:
                    num = int(folio[2:])
                    existing_numbers.add(num)
                    if num > max_number:
                        max_number = num
                except (ValueError, IndexError):
                    continue

            self.stdout.write(f"  -> A OS mais alta encontrada no banco é: OS{max_number}")

            all_possible_numbers = set(range(1, max_number + 1))
            missing_numbers = sorted(list(all_possible_numbers - existing_numbers))

            if not missing_numbers:
                self.stdout.write(self.style.SUCCESS("✅ Nenhuma lacuna encontrada. A sequência está completa!"))
                return

            self.stdout.write(self.style.WARNING(f"  -> Encontradas {len(missing_numbers)} OS faltantes na sequência."))

            resultados = self._executar_busca_paralela(missing_numbers)
            self._processar_resultados_finais(resultados)

            self.stdout.write(self.style.SUCCESS("\n" + "=" * 60))
            self.stdout.write(self.style.SUCCESS("✅ PROCESSO DE VALIDAÇÃO FINALIZADO!"))
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
                self.stdout.write(self.style.SUCCESS("✅ Token obtido!"))
            except requests.exceptions.RequestException as e:
                raise Exception(f"Erro ao obter token: {e}")

    def _fetch_os_data(self, os_number):
        wo_folio = f"OS{os_number}"
        url = f"{self.BASE_URL}/work_orders/{wo_folio}"
        headers = {"Authorization": f"Bearer {self.token_storage['token']}"}
        try:
            response = requests.get(url, headers=headers, timeout=20)
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

    def _executar_busca_paralela(self, lista_os_numeros):
        resultados = []
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = {executor.submit(self._fetch_os_data, num): num for num in lista_os_numeros}
            for future in tqdm(as_completed(futures), total=len(lista_os_numeros), desc="Buscando OS Faltantes"):
                resultados.append(future.result())
        return resultados

    def _processar_resultados_finais(self, resultados):
        importadas, nao_encontradas, com_erro = 0, 0, 0
        for res in resultados:
            if res['status'] == 'SUCCESS':
                # --- INÍCIO DA CORREÇÃO ---
                api_response_data = res.get('data')

                if not api_response_data or not isinstance(api_response_data, dict):
                    self.stdout.write(
                        self.style.WARNING(f"\nResposta inesperada da API para OS{res['os_number']}. Pulando."))
                    com_erro += 1
                    continue

                # Pega a lista de tarefas. Se for None, transforma em uma lista vazia `[]`
                lista_de_tarefas = api_response_data.get('data') or []

                if not lista_de_tarefas:
                    self.stdout.write(self.style.NOTICE(
                        f"\nOS{res['os_number']} encontrada, mas não possui tarefas associadas. Nenhum dado para importar."))
                else:
                    for item in lista_de_tarefas:
                        self._atualizar_db_com_item(item)
                    importadas += 1
                # --- FIM DA CORREÇÃO ---

            elif res['status'] == '404':
                nao_encontradas += 1
            else:
                self.stdout.write(self.style.WARNING(f"\nFalha ao buscar OS{res['os_number']}: {res.get('error')}"))
                com_erro += 1

        self.stdout.write(self.style.SUCCESS(f"\nResumo do Preenchimento de Lacunas:"))
        self.stdout.write(f"  - {importadas} OS faltantes foram importadas com sucesso.")
        self.stdout.write(f"  - {nao_encontradas} números de OS não existem na API (404).")
        self.stdout.write(f"  - {com_erro} OS falharam durante a busca ou tiveram resposta inválida.")

    def _atualizar_db_com_item(self, item):
        # Esta função é idêntica à dos outros scripts, garantindo consistência
        wo_folio = item.get('wo_folio')
        # ... (O resto desta função continua exatamente o mesmo)
        id_tarefa_api = item.get('id_work_orders_tasks')
        if not wo_folio or not id_tarefa_api: return
        data_criacao = self._parse_e_converter_datetime(item.get("creation_date"))
        data_finalizacao = self._parse_e_converter_datetime(item.get("wo_final_date"))
        data_inicio = self._parse_e_converter_datetime(item.get("initial_date"))
        id_request = item.get("id_request")
        dados_os = {
            'Status': self._converter_status(item.get("id_status_work_order")),
            'Nivel_de_Criticidade': self._converter_criticidade(item.get("id_priorities")),
            'Criado_Por': item.get("created_by"), 'Avanco_da_OS': item.get("completed_percentage"),
            'Ticket_ID': id_request, 'Possui_Ticket': "Sim" if id_request is not None else "Não",
            'Local_Empresa': item.get("parent_description"), 'Observacao_OS': item.get("task_note"),
            'Data_Criacao_OS': data_criacao, 'Ano_Criacao': data_criacao.year if data_criacao else None,
            'Mes_Criacao': data_criacao.month if data_criacao else None,
            'Dia_Criacao': data_criacao.day if data_criacao else None,
            'Hora_Criacao': data_criacao.time() if data_criacao else None, 'Data_Finalizacao_OS': data_finalizacao,
            'Ano_Finalizacao': data_finalizacao.year if data_finalizacao else None,
            'Mes_Finalizacao': data_finalizacao.month if data_finalizacao else None,
            'Dia_Finalizacao': data_finalizacao.day if data_finalizacao else None,
            'Hora_Finalizacao': data_finalizacao.time() if data_finalizacao else None,
            'Data_Iniciou_OS': data_inicio, 'Ano_Inicio': data_inicio.year if data_inicio else None,
            'Mes_Inicio': data_inicio.month if data_inicio else None,
            'Dia_Inicio': data_inicio.day if data_inicio else None,
            'Hora_Inicio': data_inicio.time() if data_inicio else None,
        }
        os_obj, _ = OrdemDeServico.objects.update_or_create(OS=wo_folio, defaults=dados_os)
        dados_tarefa = {
            'ordem_de_servico': os_obj, 'Ativo': item.get("items_log_description"),
            'Responsavel': item.get("personnel_description"), 'Plano_de_Tarefas': item.get("description"),
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