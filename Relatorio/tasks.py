from background_task import background
from django.core.management import call_command
from django.utils import timezone
import logging
from background_task.models import Task # Importar Task se for usar repeat

# Configura um logger básico (ainda útil para erros detalhados)
logger = logging.getLogger(__name__)

# Tarefa para rodar sync_fracttal (Ex: a cada 60 minutos)
@background(schedule=10)
def run_sync_fracttal():
    # --- PRINT ADICIONADO ---
    print(f"[{timezone.now().strftime('%Y-%m-%d %H:%M:%S')}] background_task: Iniciando run_sync_fracttal...")
    # --- FIM PRINT ---
    logger.info(f"Iniciando tarefa agendada: sync_fracttal às {timezone.now()}") # Log opcional
    try:
        call_command('sync_fracttal')
        # --- PRINT ADICIONADO ---
        print(f"[{timezone.now().strftime('%Y-%m-%d %H:%M:%S')}] background_task: Chamada a sync_fracttal concluída.")
        # --- FIM PRINT ---
        logger.info("Tarefa sync_fracttal concluída com sucesso.") # Log opcional
    except Exception as e:
        # --- PRINT ADICIONADO ---
        print(f"[{timezone.now().strftime('%Y-%m-%d %H:%M:%S')}] background_task: ERRO ao chamar sync_fracttal: {e}")
        # --- FIM PRINT ---
        logger.error(f"Erro ao executar sync_fracttal: {e}", exc_info=True) # Log detalhado do erro

# Tarefa para rodar update_active_os (Ex: a cada 15 minutos)
@background(schedule=10)
def run_update_active_os():
    # --- PRINT ADICIONADO ---
    print(f"[{timezone.now().strftime('%Y-%m-%d %H:%M:%S')}] background_task: Iniciando run_update_active_os...")
    # --- FIM PRINT ---
    logger.info(f"Iniciando tarefa agendada: update_active_os às {timezone.now()}") # Log opcional
    try:
        call_command('update_active_os')
        # --- PRINT ADICIONADO ---
        print(f"[{timezone.now().strftime('%Y-%m-%d %H:%M:%S')}] background_task: Chamada a update_active_os concluída.")
        # --- FIM PRINT ---
        logger.info("Tarefa update_active_os concluída com sucesso.") # Log opcional
    except Exception as e:
        # --- PRINT ADICIONADO ---
        print(f"[{timezone.now().strftime('%Y-%m-%d %H:%M:%S')}] background_task: ERRO ao chamar update_active_os: {e}")
        # --- FIM PRINT ---
        logger.error(f"Erro ao executar update_active_os: {e}", exc_info=True) # Log detalhado do erro

# Tarefa para rodar validate_os_sequence (Ex: uma vez por dia, às 03:00)
# Usando repeat=Task.DAILY para simplificar
@background(schedule=60)
def run_validate_os_sequence():
    now = timezone.now()
    # --- PRINT ADICIONADO ---
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] background_task: Verificando run_validate_os_sequence...")
    # --- FIM PRINT ---
    logger.info(f"Iniciando tarefa agendada: validate_os_sequence às {now}") # Log opcional

    try:
        # Verifica se já é (ou passou das) 3 da manhã para rodar
        run_time = now.replace(hour=3, minute=0, second=0, microsecond=0)
        # Calcula o próximo horário das 3 da manhã
        next_run_time = run_time + timezone.timedelta(days=1) if now >= run_time else run_time

        # Só executa se a hora atual for 3 da manhã ou depois
        if now >= run_time:
             print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] background_task: Executando validate_os_sequence (após 3h).")
             call_command('validate_os_sequence')
             # --- PRINT ADICIONADO ---
             print(f"[{timezone.now().strftime('%Y-%m-%d %H:%M:%S')}] background_task: Chamada a validate_os_sequence concluída.")
             # --- FIM PRINT ---
             logger.info("Tarefa validate_os_sequence concluída com sucesso.") # Log opcional
        else:
             print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] background_task: Ainda não são 3h, pulando execução de validate_os_sequence.")

        # Reagendar para o próximo dia às 3 da manhã SEMPRE
        print(f"[{timezone.now().strftime('%Y-%m-%d %H:%M:%S')}] background_task: Reagendando validate_os_sequence para {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
        run_validate_os_sequence(schedule=next_run_time) # Chama a si mesma com o novo schedule

    except Exception as e:
        # --- PRINT ADICIONADO ---
        print(f"[{timezone.now().strftime('%Y-%m-%d %H:%M:%S')}] background_task: ERRO ao chamar/reagendar validate_os_sequence: {e}")
        # --- FIM PRINT ---
        logger.error(f"Erro ao executar validate_os_sequence: {e}", exc_info=True) # Log detalhado do erro
        # Tentar reagendar mesmo em caso de erro para não parar o ciclo
        try:
             next_run_time_on_error = timezone.now().replace(hour=3, minute=0, second=0, microsecond=0) + timezone.timedelta(days=1)
             print(f"[{timezone.now().strftime('%Y-%m-%d %H:%M:%S')}] background_task: Tentando reagendar validate_os_sequence após erro para {next_run_time_on_error.strftime('%Y-%m-%d %H:%M:%S')}")
             run_validate_os_sequence(schedule=next_run_time_on_error)
        except Exception as schedule_error:
             print(f"[{timezone.now().strftime('%Y-%m-%d %H:%M:%S')}] background_task: FALHA CRÍTICA ao tentar reagendar validate_os_sequence após erro: {schedule_error}")
             logger.critical(f"Falha ao reagendar validate_os_sequence: {schedule_error}", exc_info=True)