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
from Relatorio.models import OrdemDeServico


class Command(BaseCommand):
    help = 'Verifica OS com datas de verificação/programação faltantes e preenche buscando na API.'

    TOKEN_URL = "https://app.fracttal.com/oauth/token"
    BASE_URL = "https://app.fracttal.com/api"
    MAX_WORKERS = 15

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.token_storage = {'token': None, 'lock': threading.Lock()}

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("=" * 60))
        self.stdout.write(self.style.SUCCESS("🚀 INICIANDO PREENCHIMENTO DE DATAS FALTANTES 🚀"))
        self.stdout.write(self.style.SUCCESS("=" * 60))

        try:
            self._obter_token_acesso()

            os_com_datas_faltantes = OrdemDeServico.objects.filter(
                Q(Data_Enviado_Verificacao__isnull=True) | Q(Data_Programada__isnull=True)
            )

            lista_os_folios = list(os_com_datas_faltantes.values_list('OS', flat=True))
            total_a_verificar = len(lista_os_folios)

            if total_a_verificar == 0:
                self.stdout.write(self.style.SUCCESS("✅ Nenhuma OS com datas faltantes encontrada. Tudo em dia!"))
                return

            self.stdout.write(f"🔍 Encontradas {total_a_verificar} OS com datas a serem preenchidas.")
            resultados = self._executar_busca_paralela(lista_os_folios)
            self._processar_resultados_finais(resultados)

            self.stdout.write(self.style.SUCCESS("\n" + "=" * 60))
            self.stdout.write(self.style.SUCCESS("✅ PROCESSO DE PREENCHIMENTO FINALIZADO!"))
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

    def _fetch_os_data(self, wo_folio):
        url = f"{self.BASE_URL}/work_orders/{wo_folio}"
        headers = {"Authorization": f"Bearer {self.token_storage['token']}"}
        try:
            response = requests.get(url, headers=headers, timeout=20)
            if response.status_code == 401:
                self._obter_token_acesso()
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
            for future in tqdm(as_completed(futures), total=len(lista_os_folios), desc="Buscando Datas Faltantes"):
                resultados.append(future.result())
        return resultados

    def _processar_resultados_finais(self, resultados):
        atualizadas = 0
        com_erro = 0

        for res in resultados:
            if res['status'] == 'SUCCESS':
                data_verificacao = None
                data_programada = None

                # A resposta da API pode ser nula ou não ser um dicionário
                api_data = res.get('data')
                if not api_data or not isinstance(api_data, dict):
                    com_erro += 1
                    continue

                # Percorre TODAS as tarefas retornadas para encontrar as datas
                for item in api_data.get('data', []):
                    if not data_verificacao and item.get("review_date"):
                        data_verificacao = self._parse_e_converter_datetime(item.get("review_date"))

                    if not data_programada and item.get("date_maintenance"):
                        data_programada = self._parse_e_converter_datetime(item.get("date_maintenance"))

                    # Se já encontrou as duas, pode parar de procurar
                    if data_verificacao and data_programada:
                        break

                dados_para_atualizar = {}
                if data_verificacao:
                    dados_para_atualizar['Data_Enviado_Verificacao'] = data_verificacao
                if data_programada:
                    dados_para_atualizar['Data_Programada'] = data_programada

                if dados_para_atualizar:
                    OrdemDeServico.objects.filter(OS=res['wo_folio']).update(**dados_para_atualizar)
                    atualizadas += 1
            else:
                self.stdout.write(self.style.WARNING(
                    f"\nFalha ao buscar {res['wo_folio']}: {res.get('error', 'Status ' + res.get('status', ''))}"))
                com_erro += 1

        self.stdout.write(self.style.SUCCESS(f"\nResumo do Preenchimento:"))
        self.stdout.write(f"  - {atualizadas} OS tiveram pelo menos uma data atualizada.")
        self.stdout.write(f"  - {com_erro} OS falharam durante a busca na API ou tiveram resposta inválida.")

    def _parse_e_converter_datetime(self, date_string):
        if not date_string: return None
        try:
            utc_time = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
            return utc_time.astimezone(pytz.timezone("America/Sao_Paulo"))
        except (ValueError, TypeError):
            return None