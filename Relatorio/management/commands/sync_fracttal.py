import requests
import time
from datetime import datetime
import pytz

from django.core.management.base import BaseCommand
from django.conf import settings
from Relatorio.models import OrdemDeServico, Tarefa


class Command(BaseCommand):
    help = 'Busca dados da API Fracttal e salva nos modelos relacionados de OS e Tarefas.'

    TOKEN_URL = "https://app.fracttal.com/oauth/token"
    BASE_URL = "https://app.fracttal.com/api"

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("=" * 60))
        self.stdout.write(self.style.SUCCESS("🚀 INICIANDO SINCRONIZAÇÃO TOTAL COM A API FRACTTAL 🚀"))
        self.stdout.write(self.style.SUCCESS("=" * 60))
        try:
            token = self._obter_token_acesso()
            self._buscar_e_salvar_work_orders(token)
            self.stdout.write(self.style.SUCCESS("\n" + "=" * 60))
            self.stdout.write(self.style.SUCCESS("✅ PROCESSO DE SINCRONIZAÇÃO FINALIZADO COM SUCESSO!"))
            self.stdout.write(self.style.SUCCESS("=" * 60))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"\n❌ ERRO CRÍTICO DURANTE A EXECUÇÃO: {e}"))

    def _obter_token_acesso(self):
        self.stdout.write("🔑 Obtendo token de acesso...")
        try:
            auth = (settings.FRACTTAL_CLIENT_ID, settings.FRACTTAL_CLIENT_SECRET)
            data = {"grant_type": "client_credentials", "scope": "api"}
            response = requests.post(self.TOKEN_URL, auth=auth, data=data)
            response.raise_for_status()
            token = response.json()["access_token"]
            self.stdout.write(self.style.SUCCESS("✅ Token obtido com sucesso!"))
            return token
        except requests.exceptions.RequestException as e:
            raise Exception(f"Erro crítico ao obter token: {e}")

    def _buscar_e_salvar_work_orders(self, token):
        pagina, por_pagina = 1, 100
        headers = {"Authorization": f"Bearer {token}"}
        tem_mais_paginas = True
        total_os_criadas, total_os_atualizadas = 0, 0
        total_tarefas_criadas, total_tarefas_atualizadas = 0, 0

        while tem_mais_paginas:
            url = f"{self.BASE_URL}/work_orders?page={pagina}&per_page={por_pagina}"
            self.stdout.write(f"\n📄 Buscando página {pagina}...")

            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                work_orders_na_pagina = response.json().get("data", [])

                if not work_orders_na_pagina:
                    self.stdout.write(self.style.WARNING("📭 Fim dos registros."))
                    tem_mais_paginas = False
                else:
                    resultados = self._processar_pagina(work_orders_na_pagina)
                    total_os_criadas += resultados['os_criadas']
                    total_os_atualizadas += resultados['os_atualizadas']
                    total_tarefas_criadas += resultados['tarefas_criadas']
                    total_tarefas_atualizadas += resultados['tarefas_atualizadas']
                    self.stdout.write(self.style.SUCCESS(
                        f"  -> OS: {resultados['os_criadas']} criadas, {resultados['os_atualizadas']} atualizadas."))
                    self.stdout.write(self.style.SUCCESS(
                        f"  -> Tarefas: {resultados['tarefas_criadas']} criadas, {resultados['tarefas_atualizadas']} atualizadas."))

                    if len(work_orders_na_pagina) < por_pagina:
                        tem_mais_paginas = False
                    else:
                        pagina += 1
                        time.sleep(0.2)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401:
                    self.stdout.write(self.style.WARNING("\n⚠️ Token expirado! Solicitando um novo..."))
                    token = self._obter_token_acesso()
                    headers["Authorization"] = f"Bearer {token}"
                    self.stdout.write(f"🔄 Tentando buscar a página {pagina} novamente...")
                    continue
                else:
                    raise Exception(f"Erro HTTP inesperado: {e}")
            except requests.exceptions.RequestException as e:
                raise Exception(f"Erro de conexão: {e}")

        self.stdout.write("-" * 60)
        self.stdout.write(f"🎉 Busca finalizada!")
        self.stdout.write(f"   Ordens de Serviço: {total_os_criadas} criadas, {total_os_atualizadas} atualizadas.")
        self.stdout.write(f"   Tarefas: {total_tarefas_criadas} criadas, {total_tarefas_atualizadas} atualizadas.")

    def _processar_pagina(self, work_orders):
        contadores = {'os_criadas': 0, 'os_atualizadas': 0, 'tarefas_criadas': 0, 'tarefas_atualizadas': 0}
        os_atualizadas_nesta_pagina = set()

        for item in work_orders:
            wo_folio = item.get('wo_folio')
            id_tarefa_api = item.get('id_work_orders_tasks')
            if not wo_folio or not id_tarefa_api:
                continue

            # --- Processa todas as datas ---
            data_criacao = self._parse_e_converter_datetime(item.get("creation_date"))
            data_finalizacao = self._parse_e_converter_datetime(item.get("wo_final_date"))
            data_inicio = self._parse_e_converter_datetime(item.get("initial_date"))
            data_verificacao = self._parse_e_converter_datetime(item.get("review_date"))  # NOVO
            data_programada = self._parse_e_converter_datetime(item.get("date_maintenance"))  # NOVO
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

                # --- NOVOS CAMPOS DE DATA ADICIONADOS ---
                'Data_Enviado_Verificacao': data_verificacao,
                'Data_Programada': data_programada,
            }

            os_obj, os_created = OrdemDeServico.objects.update_or_create(OS=wo_folio, defaults=dados_os)

            if os_created:
                contadores['os_criadas'] += 1
            elif wo_folio not in os_atualizadas_nesta_pagina:
                contadores['os_atualizadas'] += 1
                os_atualizadas_nesta_pagina.add(wo_folio)

            dados_tarefa = {
                'ordem_de_servico': os_obj,
                'Ativo': item.get("items_log_description"),
                'Responsavel': item.get("personnel_description"),
                'Plano_de_Tarefas': item.get("description"),
                'Tipo_de_Tarefa': item.get("tasks_log_task_type_main"),
                'Duracao_Minutos': self._segundos_para_minutos(item.get("real_duration")),
                'Status_da_Tarefa': item.get("task_status"),
            }

            _, tarefa_created = Tarefa.objects.update_or_create(id_tarefa_api=id_tarefa_api, defaults=dados_tarefa)

            if tarefa_created:
                contadores['tarefas_criadas'] += 1
            else:
                contadores['tarefas_atualizadas'] += 1

        return contadores

    # --- Funções de ajuda (Helpers) ---
    def _parse_e_converter_datetime(self, date_string):
        if not date_string: return None
        try:
            utc_time = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
            br_timezone = pytz.timezone("America/Sao_Paulo")
            return utc_time.astimezone(br_timezone)
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