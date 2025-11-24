# Relatorio/management/commands/export_tipos_tarefa.py

import pandas as pd
from django.core.management.base import BaseCommand
from Relatorio.models import Tarefa #
from datetime import datetime
import re

# Função para remover caracteres XML inválidos (mantida por precaução)
def sanitize_for_excel(text):
    if not isinstance(text, str):
        return text
    # Expressão regular para encontrar caracteres XML 1.0 inválidos
    illegal_xml_chars_re = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x84\x86-\x9f\ud800-\udfff\ufffe\uffff]')
    return illegal_xml_chars_re.sub('', text)

class Command(BaseCommand):
    help = ('Exporta todos os valores únicos do campo "Tipo_de_Tarefa" ' # Ajuda atualizada
            'para um arquivo Excel, removendo caracteres inválidos.')

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("=" * 60))
        self.stdout.write(self.style.SUCCESS(
            "📊 EXPORTANDO TIPOS DE TAREFA ÚNICOS PARA EXCEL 📊" # Título atualizado
        ))
        self.stdout.write(self.style.SUCCESS("=" * 60))

        try:
            self.stdout.write("Buscando tipos de tarefa únicos no banco de dados...")

            # *** ALTERAÇÃO PRINCIPAL: Consultar o campo Tipo_de_Tarefa ***
            tipos_unicos_qs = (
                Tarefa.objects
                .exclude(Tipo_de_Tarefa__isnull=True) # Exclui nulos do Tipo_de_Tarefa #
                .exclude(Tipo_de_Tarefa__exact='') # Exclui vazios do Tipo_de_Tarefa #
                .values_list('Tipo_de_Tarefa', flat=True) # Pega apenas os valores de Tipo_de_Tarefa #
                .distinct() # Garante valores únicos
                .order_by('Tipo_de_Tarefa') # Ordena alfabeticamente
            )
            # *** FIM DA ALTERAÇÃO PRINCIPAL ***

            tipos_unicos_list_raw = list(tipos_unicos_qs)

            if not tipos_unicos_list_raw:
                self.stdout.write(self.style.WARNING(
                    "Nenhum 'Tipo de Tarefa' preenchido foi encontrado no banco de dados."
                ))
                return

            # Sanitizar a lista
            tipos_unicos_list_sanitized = [sanitize_for_excel(tipo) for tipo in tipos_unicos_list_raw]
            tipos_unicos_list_final = sorted(list(set(tipos_unicos_list_sanitized)))


            self.stdout.write(f"Encontrados {len(tipos_unicos_list_final)} tipos únicos após sanitização.")

            # Criação do DataFrame
            df = pd.DataFrame(tipos_unicos_list_final, columns=['Tipo de Tarefa']) # Nome da coluna atualizado

            # Definição do nome do arquivo
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"tipos_de_tarefa_unicos_{timestamp}.xlsx" # Nome do arquivo atualizado

            # Exportação para Excel
            self.stdout.write(f"Exportando dados para o arquivo: {filename}...")
            try:
                with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='TiposUnicos') # Nome da planilha atualizado
                self.stdout.write(self.style.SUCCESS(
                    f"\n✅ Arquivo '{filename}' criado com sucesso contendo {len(tipos_unicos_list_final)} tipos." # Mensagem atualizada
                ))
            except Exception as e:
                self.stdout.write(self.style.ERROR(
                    f"\n❌ Erro ao escrever o arquivo Excel: {e}"
                ))

        except Exception as e:
            self.stdout.write(self.style.ERROR(
                f"\n❌ Erro inesperado durante a execução: {e}"
            ))

        self.stdout.write("=" * 60)