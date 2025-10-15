# Em Relatorio/admin.py

from django.contrib import admin
from django.db.models import IntegerField
from django.db.models.functions import Cast, Substr
from .models import OrdemDeServico, Tarefa


class TarefaInline(admin.TabularInline):
    model = Tarefa
    fields = ('id_tarefa_api', 'Ativo', 'Responsavel', 'Tipo_de_Tarefa', 'Duracao_Minutos', 'Status_da_Tarefa')
    readonly_fields = fields
    extra = 0
    can_delete = False


@admin.register(OrdemDeServico)
class OrdemDeServicoAdmin(admin.ModelAdmin):
    inlines = [TarefaInline]

    # Adicionado 'Local_Empresa'
    list_display = ('numero_os_ordenavel', 'Status', 'Local_Empresa', 'Avanco_da_OS', 'Nivel_de_Criticidade',
                    'Possui_Ticket')

    # Adicionado 'Local_Empresa' à busca
    search_fields = ('OS', 'Criado_Por', 'Ticket_ID', 'Local_Empresa', 'tarefas__Ativo')

    list_filter = ('Status', 'Nivel_de_Criticidade', 'Possui_Ticket', 'Ano_Criacao')

    # Adicionado 'Local_Empresa' à visualização de detalhes
    readonly_fields = (
        'OS', 'Status', 'Nivel_de_Criticidade', 'Criado_Por', 'Local_Empresa',
        'Data_Criacao_OS', 'Data_Finalizacao_OS', 'duracao_total_calculada',
        'Avanco_da_OS', 'Ticket_ID', 'Possui_Ticket'
    )

    # As funções 'get_queryset', 'numero_os_ordenavel' e 'duracao_total_calculada' permanecem as mesmas
    def get_queryset(self, request):
        qs = super().get_queryset(request)
        qs = qs.annotate(os_numero=Cast(Substr('OS', 3), output_field=IntegerField()))
        return qs.order_by('-os_numero')

    @admin.display(description='OS', ordering='os_numero')
    def numero_os_ordenavel(self, obj):
        return obj.OS

    @admin.display(description='Duração Total da OS (soma das tarefas)')
    def duracao_total_calculada(self, obj):
        from django.db.models import Sum
        total_minutos = obj.tarefas.aggregate(soma_total=Sum('Duracao_Minutos'))['soma_total']
        if total_minutos is None or total_minutos == 0: return "0 minutos"
        horas, minutos = divmod(total_minutos, 60)
        return f"{int(horas)}h {int(minutos)}min"


@admin.register(Tarefa)
class TarefaAdmin(admin.ModelAdmin):
    list_display = ('id_tarefa_api', 'ordem_de_servico', 'Ativo', 'Responsavel', 'Status_da_Tarefa')
    search_fields = ('id_tarefa_api', 'ordem_de_servico__OS', 'Ativo', 'Responsavel')
    list_filter = ('Tipo_de_Tarefa', 'Status_da_Tarefa', 'ordem_de_servico__Status')