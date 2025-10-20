from django.core.management.base import BaseCommand
from Relatorio.models import Tarefa

class Command(BaseCommand):
    help = 'Lista todos os valores únicos do campo "Plano_de_Tarefas" encontrados no banco de dados.'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("=" * 60))
        self.stdout.write(self.style.SUCCESS("📊 BUSCANDO PLANOS DE TAREFA ÚNICOS NO BANCO DE DADOS 📊"))
        self.stdout.write(self.style.SUCCESS("=" * 60))

        # A consulta ao banco de dados:
        # 1. Exclui registros onde o campo está vazio ou nulo.
        # 2. Pega apenas os valores da coluna 'Plano_de_Tarefas'.
        # 3. Garante que os valores sejam únicos (DISTINCT).
        # 4. Ordena o resultado em ordem alfabética.
        planos_de_tarefa_unicos = (
            Tarefa.objects
            .exclude(Plano_de_Tarefas__isnull=True)
            .exclude(Plano_de_Tarefas__exact='')
            .values_list('Plano_de_Tarefas', flat=True)
            .distinct()
            .order_by('Plano_de_Tarefas')
        )

        if not planos_de_tarefa_unicos:
            self.stdout.write(self.style.WARNING("Nenhum 'Plano de Tarefa' preenchido foi encontrado no banco de dados."))
            return

        # Imprime cada plano de tarefa encontrado em uma nova linha
        self.stdout.write("\nPlanos de Tarefa encontrados:\n")
        for plano_tarefa in planos_de_tarefa_unicos:
            self.stdout.write(f"  - {plano_tarefa}")
        self.stdout.write("-" * 60)

        # Imprime um resumo no final
        self.stdout.write(self.style.SUCCESS(
            f"\nOperação concluída. Foram encontrados {len(planos_de_tarefa_unicos)} planos de tarefa únicos."
        ))
        self.stdout.write("=" * 60)