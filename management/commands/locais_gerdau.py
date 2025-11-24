from django.core.management.base import BaseCommand
from Relatorio.models import OrdemDeServico
import sys

class Command(BaseCommand):
    """
    Busca locais que contenham 'gerdau', analisa *todos* os segmentos
    de cada local, e extrai apenas os nomes de unidades que, 
    após a limpeza de prefixo, começam com 'Gerdau'.
    """
    help = 'Lista nomes de unidades Gerdau únicos e validados'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS(
            "Buscando e validando nomes de unidades 'Gerdau'..."
        ))
        
        try:
            # 1. Busca todos os valores únicos de Local_Empresa
            locais_db = OrdemDeServico.objects.filter(
                Local_Empresa__icontains='gerdau'
            ).values_list('Local_Empresa', flat=True).distinct()

            if not locais_db:
                self.stdout.write(self.style.WARNING("Nenhum local encontrado contendo 'gerdau'."))
                return

            # Set para armazenar os nomes de unidades limpos e validados
            unidades_validadas = set()

            # 2. Processa cada string
            for local_str in locais_db:
                if not local_str:
                    continue

                # 3. Divide a string por '/' e analisa CADA parte
                partes = [parte.strip() for parte in local_str.split('/')]
                
                for parte_bruta in partes:
                    if not parte_bruta:
                        continue # Pula segmentos vazios

                    # 4. Verifica se a própria parte contém "gerdau"
                    if 'gerdau' in parte_bruta.lower():
                        
                        # 5. Limpa o prefixo (ex: 'K - ')
                        partes_nome = parte_bruta.split(' - ', 1)
                        
                        if len(partes_nome) > 1:
                            # Se dividiu, pega a segunda parte
                            nome_limpo = partes_nome[1].strip()
                        else:
                            # Se não dividiu, usa a parte inteira
                            nome_limpo = partes_nome[0].strip()

                        # 6. VALIDAÇÃO FINAL:
                        # Só adiciona se o nome limpo começar com "Gerdau"
                        if nome_limpo.lower().startswith('gerdau'):
                            # Adiciona o nome com o 'G' maiúsculo
                            # (ou como ele foi encontrado, se já for)
                            nome_final = nome_limpo[0].upper() + nome_limpo[1:]
                            unidades_validadas.add(nome_final)

            if not unidades_validadas:
                self.stdout.write(self.style.WARNING(
                    "Locais com 'gerdau' foram encontrados, mas nenhum segmento "
                    "passou na validação (começar com 'Gerdau' após limpeza)."
                ))
            else:
                self.stdout.write(self.style.SUCCESS(
                    "--- Unidades Gerdau Únicas Encontradas (Validadas) ---"
                ))
                for nome in sorted(list(unidades_validadas)):
                    self.stdout.write(nome)
                self.stdout.write("-----------------------------------------------------")
                self.stdout.write(self.style.SUCCESS(
                    f"Processamento concluído. {len(unidades_validadas)} unidades únicas validadas."
                ))

        except Exception as e:
            self.stdout.write(self.style.ERROR(
                f"Ocorreu um erro ao buscar ou processar os dados: {e}"
            ))
            sys.exit(1)