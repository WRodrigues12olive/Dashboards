from django.core.management.base import BaseCommand
from django.db.models import Count
from Relatorio.models import Tarefa

class Command(BaseCommand):
    help = 'Gera um relatório completo de todos os responsáveis e a contagem de OS únicas para cada um.'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(self.style.SUCCESS("📊 GERANDO RELATÓRIO DE DADOS BRUTOS DE TÉCNICOS 📊"))
        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write("Este relatório mostra a contagem de OS únicas para cada variação de nome de responsável encontrada no banco.\n")

        # 1. A consulta principal: agrupa por cada nome e conta as OS únicas.
        contagem_bruta_tecnicos = (Tarefa.objects
            .exclude(Responsavel__isnull=True).exclude(Responsavel__exact='')
            .values('Responsavel')
            .annotate(total_os=Count('ordem_de_servico', distinct=True))
            .order_by('-total_os')  # Ordena do maior para o menor
        )

        if not contagem_bruta_tecnicos:
            self.stdout.write(self.style.WARNING("Nenhum responsável encontrado no banco de dados."))
            return

        self.stdout.write("\n{:<50} | {:>15}".format("Nome do Responsável (como está no banco)", "Total de OS Únicas"))
        self.stdout.write("-" * 70)

        # 2. Imprime os resultados em formato de tabela
        for item in contagem_bruta_tecnicos:
            nome_bruto = item['Responsavel'].strip()
            total = item['total_os']
            self.stdout.write("{:<50} | {:>15}".format(nome_bruto, total))

        self.stdout.write("-" * 70)
        self.stdout.write(self.style.SUCCESS(f"\nRelatório concluído. Total de {len(contagem_bruta_tecnicos)} variações de nomes encontradas."))
        self.stdout.write("=" * 70)