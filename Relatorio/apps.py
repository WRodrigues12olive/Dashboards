from django.apps import AppConfig
from django.utils import timezone


class RelatorioConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'Relatorio'

    def ready(self):
        try:
            import Relatorio.tasks
            from Relatorio.tasks import run_sync_fracttal, run_update_active_os, run_validate_os_sequence
            from background_task.models import Task

            print("Módulo Relatorio.tasks importado com sucesso em apps.py")

            # Evita rodar este código quando o Django executa comandos (como migrate)
            # Vamos checar se o servidor está rodando (isso é um truque, pode não ser 100% mas ajuda)
            import sys
            if 'runserver' not in sys.argv and 'process_tasks' not in sys.argv:
                print("Não é 'runserver' nem 'process_tasks', pulando agendamento de tasks.")
                return

            now = timezone.now()

            # Agenda as tarefas APENAS SE elas já não estiverem na fila
            if not Task.objects.filter(task_name='Relatorio.tasks.run_sync_fracttal').exists():
                print("Agendando task 'run_sync_fracttal' pela primeira vez.")
                run_sync_fracttal(
                    repeat_seconds=1800,  # <-- Passe o argumento que a função agora espera
                    schedule=now + timezone.timedelta(seconds=10)
                )

            if not Task.objects.filter(task_name='Relatorio.tasks.run_update_active_os').exists():
                print("Agendando task 'run_update_active_os' pela primeira vez.")
                run_update_active_os(
                    repeat_seconds=1800,  # <-- Passe o argumento que a função agora espera
                    schedule=now + timezone.timedelta(seconds=10)
                )

            if not Task.objects.filter(task_name='Relatorio.tasks.run_validate_os_sequence').exists():
                print("Agendando task 'run_validate_os_sequence' pela primeira vez.")
                run_validate_os_sequence(
                    schedule=now + timezone.timedelta(seconds=60)
                )

        except ImportError:
            print("Erro ao importar Relatorio.tasks em apps.py")
            pass
        except Exception as e:
            # Captura outros erros (como o banco de dados não estar pronto)
            print(f"Erro no apps.py ready(): {e}")
            pass