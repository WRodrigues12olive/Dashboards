from django.db import migrations


def transferir_dados_observacao(apps, schema_editor):
    """
    Copia a observação da primeira tarefa encontrada para a sua Ordem de Serviço pai.
    """
    OrdemDeServico = apps.get_model('Relatorio', 'OrdemDeServico')

    # Itera sobre todas as Ordens de Serviço
    for os in OrdemDeServico.objects.all().iterator():
        # Para cada OS, encontra a primeira tarefa associada que tenha uma observação
        primeira_tarefa_com_obs = os.tarefas.filter(Observacao__isnull=False).exclude(Observacao='').first()

        # Se encontrou uma tarefa com observação, copia o valor
        if primeira_tarefa_com_obs:
            # CORRIGIDO: Usa o nome de campo correto 'Observacao'
            os.Observacao = primeira_tarefa_com_obs.Observacao
            # CORRIGIDO: Salva o campo correto 'Observacao'
            os.save(update_fields=['Observacao'])


def reverter_transferencia(apps, schema_editor):
    """
    Se precisarmos reverter, apenas limpa a nova coluna.
    """
    OrdemDeServico = apps.get_model('Relatorio', 'OrdemDeServico')
    # CORRIGIDO: Atualiza o campo correto 'Observacao'
    OrdemDeServico.objects.all().update(Observacao=None)


class Migration(migrations.Migration):
    dependencies = [
        # A dependência que você já corrigiu
        ('Relatorio', '0007_ordemdeservico_observacao'),
    ]

    operations = [
        migrations.RunPython(transferir_dados_observacao, reverse_code=reverter_transferencia),
    ]