from django.db import models


class OrdemDeServico(models.Model):
    OS = models.CharField(max_length=50, unique=True, verbose_name="OS")
    Status = models.CharField(max_length=50, blank=True, null=True, verbose_name="Status")
    Nivel_de_Criticidade = models.CharField(max_length=50, blank=True, null=True, verbose_name="Nível de Criticidade")
    Criado_Por = models.CharField(max_length=255, blank=True, null=True, verbose_name="Criado Por")
    Avanco_da_OS = models.IntegerField(blank=True, null=True, verbose_name="Avanço da OS (%)")
    Ticket_ID = models.IntegerField(blank=True, null=True, verbose_name="ID do Ticket")
    Possui_Ticket = models.CharField(max_length=3, blank=True, null=True, verbose_name="Possui Ticket?")
    Local_Empresa = models.CharField(max_length=500, blank=True, null=True, verbose_name="Local/Empresa")
    Observacao_OS = models.TextField(blank=True, null=True, verbose_name="Descrição")

    # Datas Principais
    Data_Criacao_OS = models.DateTimeField(blank=True, null=True, verbose_name="Data de Criação OS")
    Data_Finalizacao_OS = models.DateTimeField(blank=True, null=True, verbose_name="Data de Finalização OS")
    Data_Enviado_Verificacao = models.DateTimeField(blank=True, null=True, verbose_name="Data Enviado Verificação")
    Data_Programada = models.DateTimeField(blank=True, null=True, verbose_name="Data Programada")

    Data_Iniciou_OS = models.DateTimeField(blank=True, null=True, verbose_name="Data Iniciou OS")
    Ano_Inicio = models.IntegerField(blank=True, null=True)
    Mes_Inicio = models.IntegerField(blank=True, null=True)
    Dia_Inicio = models.IntegerField(blank=True, null=True)
    Hora_Inicio = models.TimeField(blank=True, null=True)

    # Campos derivados (Criação)
    Ano_Criacao = models.IntegerField(blank=True, null=True)
    Mes_Criacao = models.IntegerField(blank=True, null=True)
    Dia_Criacao = models.IntegerField(blank=True, null=True)
    Hora_Criacao = models.TimeField(blank=True, null=True)

    # Campos derivados (Finalização)
    Ano_Finalizacao = models.IntegerField(blank=True, null=True)
    Mes_Finalizacao = models.IntegerField(blank=True, null=True)
    Dia_Finalizacao = models.IntegerField(blank=True, null=True)
    Hora_Finalizacao = models.TimeField(blank=True, null=True)

    def __str__(self):
        return self.OS

    class Meta:
        verbose_name = "Ordem de Serviço"
        verbose_name_plural = "Ordens de Serviço"


class Tarefa(models.Model):
    ordem_de_servico = models.ForeignKey(OrdemDeServico, on_delete=models.CASCADE, related_name='tarefas')
    id_tarefa_api = models.IntegerField(unique=True, verbose_name="ID da Tarefa na API")
    Ativo = models.TextField(blank=True, null=True, verbose_name="Ativo")
    Responsavel = models.CharField(max_length=255, blank=True, null=True, verbose_name="Responsável")
    Plano_de_Tarefas = models.TextField(blank=True, null=True, verbose_name="Plano de Tarefas")
    Tipo_de_Tarefa = models.CharField(max_length=100, blank=True, null=True, verbose_name="Tipo de Tarefa")
    Duracao_Minutos = models.FloatField(blank=True, null=True, verbose_name="Duração (Minutos)")
    Status_da_Tarefa = models.CharField(max_length=50, blank=True, null=True, verbose_name="Status da Tarefa")

    def __str__(self):
        return f"Tarefa {self.id_tarefa_api} da {self.ordem_de_servico.OS}"

    class Meta:
        verbose_name = "Tarefa"
        verbose_name_plural = "Tarefas"
