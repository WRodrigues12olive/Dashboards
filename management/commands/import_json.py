import json
from datetime import datetime
import pytz

from django.core.management.base import BaseCommand
from Relatorio.models import OrdemDeServico, Tarefa


class Command(BaseCommand):
    help = 'Importa dados de um arquivo JSON para os modelos OrdemDeServico e Tarefa.'

    def add_arguments(self, parser):
        parser.add_argument('json_file', type=str, help='O caminho para o arquivo JSON.')

    def handle(self, *args, **options):
        json_file_path = options['json_file']
        self.stdout.write(self.style.SUCCESS(f'Iniciando importação do arquivo: {json_file_path}'))

        with open(json_file_path, 'r', encoding='utf-8') as f:
            json_content = json.load(f)

        lista_de_blocos = []
        if 'dados' in json_content and isinstance(json_content['dados'], list):
            lista_de_blocos = json_content['dados']
        else:
            self.stdout.write(self.style.ERROR("Estrutura de JSON inválida. Chave 'dados' não encontrada."))
            return

        os_criadas, os_atualizadas, tarefas_criadas, tarefas_atualizadas = 0, 0, 0, 0
        os_atualizadas_neste_run = set()

        for entry in lista_de_blocos:
            lista_de_tarefas_json = entry.get('data', [])
            if not isinstance(lista_de_tarefas_json, list):
                continue

            for item in lista_de_tarefas_json:
                resultados = self._processar_item(item, os_atualizadas_neste_run)
                if resultados:
                    if resultados['os_created']:
                        os_criadas += 1
                    elif resultados['os_updated']:
                        os_atualizadas += 1
                    if resultados['tarefa_created']:
                        tarefas_criadas += 1
                    else:
                        tarefas_atualizadas += 1

        self.stdout.write("-" * 60)
        self.stdout.write(self.style.SUCCESS('Importação concluída!'))
        self.stdout.write(f'  Ordens de Serviço: {os_criadas} criadas, {os_atualizadas} atualizadas.')
        self.stdout.write(f'  Tarefas: {tarefas_criadas} criadas, {tarefas_atualizadas} atualizadas.')

    def _processar_item(self, item, os_atualizadas_neste_run):
        wo_folio = item.get('wo_folio')
        id_tarefa_api = item.get('id_work_orders_tasks')
        if not wo_folio or not id_tarefa_api:
            return None

        data_criacao = self._parse_e_converter_datetime(item.get("creation_date"))
        data_finalizacao = self._parse_e_converter_datetime(item.get("wo_final_date"))
        data_inicio = self._parse_e_converter_datetime(item.get("initial_date"))
        id_request = item.get("id_request")

        dados_os = {
            'Status': self._converter_status(item.get("id_status_work_order")),
            'Nivel_de_Criticidade': self._converter_criticidade(item.get("id_priorities")),
            'Criado_Por': item.get("created_by"),
            'Avanco_da_OS': item.get("completed_percentage"),
            'Ticket_ID': id_request,
            'Possui_Ticket': "Sim" if id_request is not None else "Não",
            'Local_Empresa': item.get("parent_description"),
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
            'Data_Iniciou_OS': data_inicio,
            'Ano_Inicio': data_inicio.year if data_inicio else None,
            'Mes_Inicio': data_inicio.month if data_inicio else None,
            'Dia_Inicio': data_inicio.day if data_inicio else None,
            'Hora_Inicio': data_inicio.time() if data_inicio else None,
        }
        os_obj, os_created = OrdemDeServico.objects.update_or_create(OS=wo_folio, defaults=dados_os)

        os_updated = False
        if not os_created and wo_folio not in os_atualizadas_neste_run:
            os_updated = True
            os_atualizadas_neste_run.add(wo_folio)

        dados_tarefa = {
            'ordem_de_servico': os_obj,
            'Ativo': item.get("items_log_description"),
            'Responsavel': item.get("personnel_description"),
            'Plano_de_Tarefas': item.get("description"),
            'Tipo_de_Tarefa': item.get("tasks_log_task_type_main"),
            'Duracao_Minutos': self._segundos_para_minutos(item.get("real_duration")),
            'Status_da_Tarefa': item.get("task_status"),
            'Observacao': item.get("task_note"),
        }

        _, tarefa_created = Tarefa.objects.update_or_create(id_tarefa_api=id_tarefa_api, defaults=dados_tarefa)

        return {'os_created': os_created, 'os_updated': os_updated, 'tarefa_created': tarefa_created}

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