from django.contrib import admin
from django.db.models import IntegerField, Sum
from django.db.models.functions import Cast, Substr
from .models import OrdemDeServico, Tarefa


# Filtro personalizado que agrupa os locais pela empresa principal
class LocalEmpresaFilter(admin.SimpleListFilter):
    title = 'por Empresa Principal'
    parameter_name = 'empresa'

    def lookups(self, request, model_admin):
        locais = OrdemDeServico.objects.exclude(Local_Empresa__isnull=True).exclude(Local_Empresa='').values_list(
            'Local_Empresa', flat=True).distinct()

        empresas = set()
        for local in locais:
            partes = [p.strip() for p in local.split('/') if p.strip()]
            if partes:
                empresas.add(partes[0])

        return sorted([(empresa.lower(), empresa) for empresa in empresas])

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(Local_Empresa__icontains=self.value())
        return queryset


class TarefaInline(admin.TabularInline):
    model = Tarefa
    fields = ('id_tarefa_api', 'Ativo', 'Responsavel', 'Tipo_de_Tarefa', 'Duracao_Minutos', 'Status_da_Tarefa')
    readonly_fields = fields
    extra = 0
    can_delete = False


@admin.register(OrdemDeServico)
class OrdemDeServicoAdmin(admin.ModelAdmin):
    inlines = [TarefaInline]

    list_display = ('numero_os_ordenavel', 'Status', 'Local_Empresa', 'Avanco_da_OS', 'Nivel_de_Criticidade', 'Possui_Ticket')

    search_fields = ('OS', 'Criado_Por', 'Ticket_ID', 'Local_Empresa', 'tarefas__Ativo', 'tarefas__Responsavel')

    list_filter = ('Status', 'Nivel_de_Criticidade', 'Possui_Ticket', LocalEmpresaFilter, 'tarefas__Responsavel', 'Ano_Criacao')

    readonly_fields = (
        'OS', 'Status', 'Nivel_de_Criticidade', 'Criado_Por', 'Local_Empresa', 'Observacao_OS',
        'Data_Criacao_OS', 'Data_Iniciou_OS', 'Data_Finalizacao_OS', 'Data_Enviado_Verificacao', 'Data_Programada',
        'duracao_total_calculada',
        'Avanco_da_OS', 'Ticket_ID', 'Possui_Ticket'
    )

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        qs = qs.annotate(os_numero=Cast(Substr('OS', 3), output_field=IntegerField()))
        return qs.order_by('-os_numero')

    @admin.display(description='OS', ordering='os_numero')
    def numero_os_ordenavel(self, obj):
        return obj.OS

    @admin.display(description='Duração Total da OS (soma das tarefas)')
    def duracao_total_calculada(self, obj):
        total_minutos = obj.tarefas.aggregate(soma_total=Sum('Duracao_Minutos'))['soma_total']
        if not total_minutos: return "0 minutos"
        horas, minutos = divmod(total_minutos, 60)
        return f"{int(horas)}h {int(minutos)}min"


@admin.register(Tarefa)
class TarefaAdmin(admin.ModelAdmin):
    list_display = ('id_tarefa_api', 'ordem_de_servico', 'Ativo', 'Responsavel', 'Status_da_Tarefa')
    search_fields = ('id_tarefa_api', 'ordem_de_servico__OS', 'Ativo', 'Responsavel')
    list_filter = ('Tipo_de_Tarefa', 'Status_da_Tarefa', 'ordem_de_servico__Status')
