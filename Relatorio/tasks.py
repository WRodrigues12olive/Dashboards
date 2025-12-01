from background_task import background
from django.core.management import call_command
from django.utils import timezone
import logging
from background_task.models import Task

logger = logging.getLogger(__name__)

# Tarefa para rodar sync_fracttal
@background(schedule=10)
def run_sync_fracttal(repeat_seconds=None): # <-- Adicione 'repeat_seconds'
    print(f"[{timezone.now().strftime('%Y-%m-%d %H:%M:%S')}] background_task: Iniciando run_sync_fracttal...")
    logger.info(f"Iniciando tarefa agendada: sync_fracttal às {timezone.now()}")
    try:
        call_command('sync_fracttal')
        print(f"[{timezone.now().strftime('%Y-%m-%d %H:%M:%S')}] background_task: Chamada a sync_fracttal concluída.")
        logger.info("Tarefa sync_fracttal concluída com sucesso.")
    except Exception as e:
        print(f"[{timezone.now().strftime('%Y-%m-%d %H:%M:%S')}] background_task: ERRO ao chamar sync_fracttal: {e}")
        logger.error(f"Erro ao executar sync_fracttal: {e}", exc_info=True)
    finally:
        if repeat_seconds:
            next_run = timezone.now() + timezone.timedelta(seconds=repeat_seconds)
            print(f"[{timezone.now().strftime('%Y-%m-%d %H:%M:%S')}] background_task: Reagendando run_sync_fracttal para {next_run}.")
            # Chama a si mesma para a próxima execução
            run_sync_fracttal(repeat_seconds=repeat_seconds, schedule=next_run)
        # --- FIM DA CORREÇÃO ---


# Tarefa para rodar update_active_os
@background(schedule=10)
def run_update_active_os(repeat_seconds=None): # <-- Adicione 'repeat_seconds'
    print(f"[{timezone.now().strftime('%Y-%m-%d %H:%M:%S')}] background_task: Iniciando run_update_active_os...")
    logger.info(f"Iniciando tarefa agendada: update_active_os às {timezone.now()}")
    try:
        call_command('update_active_os')
        print(f"[{timezone.now().strftime('%Y-%m-%d %H:%M:%S')}] background_task: Chamada a update_active_os concluída.")
        logger.info("Tarefa update_active_os concluída com sucesso.")
    except Exception as e:
        print(f"[{timezone.now().strftime('%Y-%m-%d %H:%M:%S')}] background_task: ERRO ao chamar update_active_os: {e}")
        logger.error(f"Erro ao executar update_active_os: {e}", exc_info=True)
    finally:
        # --- INÍCIO DA CORREÇÃO ---
        # Se 'repeat_seconds' foi passado, reagende a tarefa.
        if repeat_seconds:
            next_run = timezone.now() + timezone.timedelta(seconds=repeat_seconds)
            print(f"[{timezone.now().strftime('%Y-%m-%d %H:%M:%S')}] background_task: Reagendando run_update_active_os para {next_run}.")
            # Chama a si mesma para a próxima execução
            run_update_active_os(repeat_seconds=repeat_seconds, schedule=next_run)

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