from django.apps import AppConfig


class RelatorioConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'Relatorio'

    def ready(self):
        # Importa o módulo tasks AQUI para registrar as tarefas
        try:
            import Relatorio.tasks
            print("Módulo Relatorio.tasks importado com sucesso em apps.py") # Confirmação
        except ImportError:
            print("Erro ao importar Relatorio.tasks em apps.py")
            pass
