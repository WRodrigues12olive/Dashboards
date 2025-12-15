from django.core.management.base import BaseCommand
from Relatorio.models import OrdemDeServico, Tarefa
import time

class Command(BaseCommand):
    help = 'Atualiza classificações e aplica regra do HC'

    def handle(self, *args, **kwargs):
        start_time = time.time()
        
        print("1. Normalizando Ordens de Serviço (Baseado no Local)...")
        for os in OrdemDeServico.objects.all().iterator():
            os.save() 

        print("2. Normalizando Tarefas e Aplicando Regra do HC nas OSs...")
        total_tarefas = Tarefa.objects.count()
        count = 0
        for tarefa in Tarefa.objects.all().iterator():
            tarefa.save()
            count += 1
            if count % 1000 == 0:
                print(f"Tarefas processadas: {count}/{total_tarefas}")

        elapsed = time.time() - start_time
        print(f"Concluído! Dados normalizados e Regra HC aplicada em {elapsed:.2f}s")