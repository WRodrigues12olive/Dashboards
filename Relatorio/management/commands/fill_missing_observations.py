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
    help = 'Verifica OS sem observação no banco de dados e tenta preenchê-las buscando na API da Fracttal.'

    TOKEN_URL = "https://app.fracttal.com/oauth/token"
    BASE_URL = "https://app.fracttal.com/api"
    MAX_WORKERS = 15

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.token_storage = {'token': None, 'lock': threading.Lock()}

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("=" * 60))
        self.stdout.write(self.style.SUCCESS("🚀 INICIANDO PREENCHIMENTO DE OBSERVAÇÕES FALTANTES 🚀"))
        self.stdout.write(self.style.SUCCESS("=" * 60))

        try:
            self._obter_token_acesso()

            # 1. Busca no banco de dados local as OS com observação vazia ou nula
            os_sem_observacao = OrdemDeServico.objects.filter(
                Q(Observacao_OS__isnull=True) | Q(Observacao_OS='')
            )

            lista_os_folios = list(os_sem_observacao.values_list('OS', flat=True))
            total_a_verificar = len(lista_os_folios)

            if total_a_verificar == 0:
                self.stdout.write(self.style.SUCCESS("✅ Nenhuma OS com observação faltante encontrada. Tudo em dia!"))
                return

            self.stdout.write(f"🔍 Encontradas {total_a_verificar} OS sem observação para verificar na API.")

            # 2. Executa a busca paralela na API para essas OS
            resultados = self._executar_busca_paralela(lista_os_folios)

            # 3. Processa os resultados e atualiza o banco
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
        """Busca os dados de uma única OS na API."""
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
            for future in tqdm(as_completed(futures), total=len(lista_os_folios), desc="Buscando Observações"):
                resultados.append(future.result())
        return resultados

    def _processar_resultados_finais(self, resultados):
        preenchidas = 0
        sem_nota_na_api = 0
        com_erro = 0

        for res in resultados:
            if res['status'] == 'SUCCESS':
                # Procura pela primeira 'task_note' não vazia na resposta da API
                observacao_encontrada = None
                for item in res['data'].get('data', []):
                    task_note = item.get("task_note")
                    if task_note:
                        observacao_encontrada = task_note
                        break  # Encontrou a primeira, não precisa procurar mais

                if observacao_encontrada:
                    # Atualiza o campo no banco de dados
                    OrdemDeServico.objects.filter(OS=res['wo_folio']).update(Observacao_OS=observacao_encontrada)
                    preenchidas += 1
                else:
                    # A OS foi encontrada, mas não tinha nenhuma observação
                    sem_nota_na_api += 1
            else:
                self.stdout.write(self.style.WARNING(
                    f"\nFalha ao buscar {res['wo_folio']}: {res.get('error', 'Status ' + res.get('status', ''))}"))
                com_erro += 1

        self.stdout.write(self.style.SUCCESS(f"\nResumo da Verificação:"))
        self.stdout.write(f"  - {preenchidas} OS tiveram suas observações preenchidas.")
        self.stdout.write(f"  - {sem_nota_na_api} OS foram encontradas na API, mas não possuíam observação.")
        self.stdout.write(f"  - {com_erro} OS falharam durante a busca na API.")