from django.core.management.base import BaseCommand
from Relatorio.models import OrdemDeServico

class Command(BaseCommand):
    help = 'Lista todos os valores únicos do campo "Local_Empresa" encontrados no banco de dados.'

    def handle(self, *args, **options):
        self.stdout.write("Buscando todos os locais únicos no banco de dados...")

        # A consulta ao banco de dados:
        # 1. Exclui registros onde o campo está vazio ou nulo.
        # 2. Pega apenas os valores da coluna 'Local_Empresa'.
        # 3. Garante que os valores sejam únicos (DISTINCT).
        # 4. Ordena o resultado em ordem alfabética.
        locais_unicos = (
            OrdemDeServico.objects
            .exclude(Local_Empresa__isnull=True)
            .exclude(Local_Empresa='')
            .values_list('Local_Empresa', flat=True)
            .distinct()
            .order_by('Local_Empresa')
        )

        if not locais_unicos:
            self.stdout.write(self.style.WARNING("Nenhum local preenchido foi encontrado no banco de dados."))
            return

        # Imprime cada local encontrado em uma nova linha
        self.stdout.write("-" * 50)
        for local in locais_unicos:
            self.stdout.write(local)
        self.stdout.write("-" * 50)

        # Imprime um resumo no final
        self.stdout.write(self.style.SUCCESS(
            f"\nOperação concluída. Foram encontrados {len(locais_unicos)} locais únicos."
        ))