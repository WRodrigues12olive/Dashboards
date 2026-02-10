from django.core.management.base import BaseCommand
from Relatorio.models import Tarefa
import time

class Command(BaseCommand):
    help = 'Atualiza apenas o agrupamento dos nomes dos técnicos (Responsavel_Agrupado) baseando-se no mapping atual.'

    def handle(self, *args, **kwargs):
        start_time = time.time()
        print("Iniciando atualização exclusiva dos Técnicos (Responsavel_Agrupado)...")

        qs = Tarefa.objects.all()
        total_tarefas = qs.count()
        count = 0

        for tarefa in qs.iterator():
            tarefa.save(update_fields=['Responsavel_Agrupado'])
            
            count += 1
            if count % 1000 == 0:
                print(f"Tarefas processadas: {count}/{total_tarefas}")

        elapsed = time.time() - start_time
        print(f"Concluído! {count} técnicos atualizados com o novo mapeamento em {elapsed:.2f}s")