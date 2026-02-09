from django.contrib.auth.models import User
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.db import models
from .mappings import get_grupo_local, get_local_detalhado, get_grupo_tecnico, get_grupo_tipo_tarefa, get_trt_specific_name
from django.db.models import Count, F, Q, Sum, Case, When, Value, DurationField, ExpressionWrapper
from django.db.models.functions import Coalesce, TruncMonth
from datetime import timedelta

class OrdemDeServicoQuerySet(models.QuerySet):
    def no_periodo(self, start_date, end_date):
        if start_date and end_date:
            return self.filter(Data_Criacao_OS__range=(start_date, end_date))
        return self

    def ativas(self):
        return self.exclude(Status__in=['Concluído', 'Cancelado'])

    def com_tempo_execucao(self):
        """Calcula o tempo de execução diretamente no Banco"""
        return self.annotate(
            tempo_execucao=ExpressionWrapper(
                F('Data_Enviado_Verificacao') - F('Data_Criacao_OS'),
                output_field=DurationField()
            )
        )

    def por_grupo_local(self):
        """Agrupa usando o campo já calculado no save()"""
        return self.values('Local_Agrupado').annotate(total=Count('id')).order_by('-total')
    def anotada_com_sla(self):
        
        return self.com_tempo_execucao().annotate(
            violou_sla=Case(
                When(
                    Q(Local_Agrupado='Gerdau') & 
                    Q(tempo_execucao__gt=timedelta(hours=24)), 
                    then=Value(True)
                ),
                
                When(
                    Q(Local_Agrupado='Park Shopping Canoas') & 
                    Q(tempo_execucao__gt=timedelta(hours=24)),
                    then=Value(True)
                ),
                default=Value(False),
                output_field=models.BooleanField()
            )
        )

class OrdemDeServicoManager(models.Manager):
    def get_queryset(self):
        return OrdemDeServicoQuerySet(self.model, using=self._db)

    def metricas_gerais(self, start_date=None, end_date=None):
        qs = self.get_queryset().no_periodo(start_date, end_date)
        return qs.aggregate(
            total=Count('id'),
            concluidas=Count('id', filter=Q(Status='Concluído')),
            canceladas=Count('id', filter=Q(Status='Cancelado')),
            em_processo=Count('id', filter=Q(Status='Em Processo')),
            em_verificacao=Count('id', filter=Q(Status='Em Verificação')),
        )

class Profile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='profile')
    force_password_change = models.BooleanField(default=True, verbose_name="Obrigar Troca de Senha")

    def __str__(self):
        return f"Perfil de {self.user.username}"

@receiver(post_save, sender=User)
def create_user_profile(sender, instance, created, **kwargs):
    if created:
        Profile.objects.create(user=instance)

@receiver(post_save, sender=User)
def save_user_profile(sender, instance, **kwargs):
    if hasattr(instance, 'profile'):
        instance.profile.save()

class OrdemDeServico(models.Model):
    OS = models.CharField(max_length=50, unique=True, verbose_name="OS")
    Status = models.CharField(max_length=50, blank=True, null=True, verbose_name="Status")
    Nivel_de_Criticidade = models.CharField(max_length=50, blank=True, null=True, verbose_name="Nível de Criticidade")
    Criado_Por = models.CharField(max_length=255, blank=True, null=True, verbose_name="Criado Por")
    Avanco_da_OS = models.IntegerField(blank=True, null=True, verbose_name="Avanço da OS (%)")
    Ticket_ID = models.IntegerField(blank=True, null=True, verbose_name="ID do Ticket")
    Possui_Ticket = models.CharField(max_length=3, blank=True, null=True, verbose_name="Possui Ticket?")
    Local_Empresa = models.CharField(max_length=500, blank=True, null=True, verbose_name="Local/Empresa")
    
    Local_Agrupado = models.CharField(max_length=100, blank=True, null=True, verbose_name="Cliente (Agrupado)", db_index=True)
    
    Local_Detalhado = models.CharField(max_length=100, blank=True, null=True, verbose_name="Local Detalhado", db_index=True)

    Observacao_OS = models.TextField(blank=True, null=True, verbose_name="Descrição")

    Data_Criacao_OS = models.DateTimeField(blank=True, null=True, verbose_name="Data de Criação OS")
    Data_Finalizacao_OS = models.DateTimeField(blank=True, null=True, verbose_name="Data de Finalização OS")
    Data_Enviado_Verificacao = models.DateTimeField(blank=True, null=True, verbose_name="Data Enviado Verificação")
    Data_Programada = models.DateTimeField(blank=True, null=True, verbose_name="Data Programada")
    Data_Iniciou_OS = models.DateTimeField(blank=True, null=True, verbose_name="Data Iniciou OS")
    
    Ano_Inicio = models.IntegerField(blank=True, null=True)
    Mes_Inicio = models.IntegerField(blank=True, null=True)
    Dia_Inicio = models.IntegerField(blank=True, null=True)
    Hora_Inicio = models.TimeField(blank=True, null=True)

    Ano_Criacao = models.IntegerField(blank=True, null=True)
    Mes_Criacao = models.IntegerField(blank=True, null=True)
    Dia_Criacao = models.IntegerField(blank=True, null=True)
    Hora_Criacao = models.TimeField(blank=True, null=True)

    Ano_Finalizacao = models.IntegerField(blank=True, null=True)
    Mes_Finalizacao = models.IntegerField(blank=True, null=True)
    Dia_Finalizacao = models.IntegerField(blank=True, null=True)
    Hora_Finalizacao = models.TimeField(blank=True, null=True)

    objects = OrdemDeServicoManager()

    def save(self, *args, **kwargs):
        if self.Local_Empresa:
            self.Local_Agrupado = get_grupo_local(self.Local_Empresa)
            self.Local_Detalhado = get_local_detalhado(self.Local_Empresa)
        else:
            self.Local_Agrupado = 'Outros'
            self.Local_Detalhado = 'Outros'

        if self.pk:
            tem_tarefa_hc = self.tarefas.filter(Ativo__icontains='HOSPITAL DE CLINICAS').exists()
            if tem_tarefa_hc:
                self.Local_Agrupado = 'Hospital De Clinicas'
                self.Local_Detalhado = 'Hospital De Clinicas'
                
        if self.pk and (self.Local_Detalhado == 'TRT Outros' or self.Local_Agrupado == 'TRT'):
             for tarefa in self.tarefas.all():
                 if tarefa.Ativo:
                     novo_detalhe = get_trt_specific_name(tarefa.Ativo)
                     if novo_detalhe != 'TRT Outros':
                         self.Local_Detalhado = novo_detalhe
                         break

        if self.Data_Criacao_OS:
            self.Ano_Criacao = self.Data_Criacao_OS.year
            self.Mes_Criacao = self.Data_Criacao_OS.month
            self.Dia_Criacao = self.Data_Criacao_OS.day
            self.Hora_Criacao = self.Data_Criacao_OS.time()
            
        if self.Data_Iniciou_OS:
            self.Ano_Inicio = self.Data_Iniciou_OS.year
            self.Mes_Inicio = self.Data_Iniciou_OS.month
            self.Dia_Inicio = self.Data_Iniciou_OS.day
            self.Hora_Inicio = self.Data_Iniciou_OS.time()

        if self.Data_Finalizacao_OS:
            self.Ano_Finalizacao = self.Data_Finalizacao_OS.year
            self.Mes_Finalizacao = self.Data_Finalizacao_OS.month
            self.Dia_Finalizacao = self.Data_Finalizacao_OS.day
            self.Hora_Finalizacao = self.Data_Finalizacao_OS.time()

        super(OrdemDeServico, self).save(*args, **kwargs)

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
    types_description = models.TextField(blank=True, null=True, verbose_name="Descrição do Tipo")
    causes_description = models.TextField(blank=True, null=True, verbose_name="Descrição da Causa")
    detection_method_description = models.TextField(blank=True, null=True, verbose_name="Método de Detecção")
    
    Responsavel_Agrupado = models.CharField(max_length=100, blank=True, null=True, verbose_name="Responsável (Grupo)", db_index=True)
    Tipo_Tarefa_Agrupado = models.CharField(max_length=100, blank=True, null=True, verbose_name="Tipo de Tarefa (Grupo)", db_index=True)

    Duracao_Minutos = models.FloatField(blank=True, null=True, verbose_name="Duração (Minutos)")
    Status_da_Tarefa = models.CharField(max_length=50, blank=True, null=True, verbose_name="Status da Tarefa")

    def save(self, *args, **kwargs):
        self.Responsavel_Agrupado = get_grupo_tecnico(self.Responsavel)
        self.Tipo_Tarefa_Agrupado = get_grupo_tipo_tarefa(self.Tipo_de_Tarefa)

        super(Tarefa, self).save(*args, **kwargs)

        os_update_fields = []
        os = self.ordem_de_servico
        
        if self.Ativo and 'HOSPITAL DE CLINICAS' in self.Ativo.upper():
            if os.Local_Agrupado != 'Hospital De Clinicas':
                os.Local_Agrupado = 'Hospital De Clinicas'
                os.Local_Detalhado = 'Hospital De Clinicas'
                os_update_fields.extend(['Local_Agrupado', 'Local_Detalhado'])

        if self.Ativo and ('TRT' in self.Ativo.upper() or '4 REGIAO' in self.Ativo.upper()):
            if os.Local_Agrupado != 'TRT' or os.Local_Detalhado == 'TRT Outros':
                novo_local = get_trt_specific_name(self.Ativo)
                
                if novo_local != 'TRT Outros':
                    os.Local_Agrupado = 'TRT'
                    os.Local_Detalhado = novo_local
                    os_update_fields.extend(['Local_Agrupado', 'Local_Detalhado'])

        if os_update_fields:
            os.save(update_fields=list(set(os_update_fields)))

    def __str__(self):
        return f"Tarefa {self.id_tarefa_api} da {self.ordem_de_servico.OS}"

    class Meta:
        verbose_name = "Tarefa"
        verbose_name_plural = "Tarefas"