from django.shortcuts import render
from django.db.models import Count, F, Q
from django.db.models.functions import TruncMonth, ExtractWeek, Now
from .models import OrdemDeServico, Tarefa
from datetime import datetime, timedelta
from django.http import JsonResponse, HttpResponse
from django.shortcuts import redirect
from django.urls import reverse
import pandas as pd
import io
import re
from collections import defaultdict

# --- LÓGICA CENTRALIZADA DE AGRUPAMENTO POR LOCAL (sem alterações) ---
KEYWORDS_LOCAIS = [
    "ADAMA", "ADM BRASIL", "ALPHAVILLE", "ARCELORMITTAL", "ASSEMBLEIA", "BALL", "BIC", "CPFL", "CSN",
    "ENGELOG", "FITESA", "FUNDAÇÃO BANRISUL", "GERDAU", "HOSPITAL DE CLINICAS", "LOJA MAÇONICA", "M DIAS", "NEOENERGIA",
    "PORTO DO AÇU", "RAIZEN", "SICOOB", "SIMEC", "SUMESA", "TRÊS CORAÇÕES", "TRT4", "TURIS", "UNILEVER",
    "UFRGS IPH", "UFRGS", "PARK SHOPPING CANOAS", "CANOAS"
]

MAPEAMENTO_TAREFAS = {
    "Corretiva": {
        "acompanhamento de atividade de terceiros", "corretiva", "corretiva teste",
        "corretiva com análise de falha", "corretiva/preventiva/instalação",
        "corretivo cartão sd card", "corretivo de caixa econômica federal", "correto",
        "correção de falha", "garantia", "manutenção corretiva do equipamentos de alarme e c",
        "manutenção corretiva do equipamentos de cftv", "manutenção corretivo cliente avulso",
        "manutenção de garantia da implantação", "plano corretivo cliente contrato",
        "plano corretivo cliente contrato emergencial", "substituição",
        "substituição de equipamento", "troca de equipamento"
    },
    "Preventiva": {
        "autorização preventiva", "checklist servidor", "limpeza de equipamentos",
        "manutenção preventiva cftv", "manutenção preventiva do equipamentos de rede - ra",
        "plano preventivo cliente contrato", "preventiva", "preventiva câmeras",
        "preventiva neo", "programação de manutenção",
        "programação de manutenção de segurança eletrônica",
        "programação de manutenção de telefônica", "programações", "checklist"
    },
    "Instalação": {
        "fornecimento de mão de obra - projetos", "instalação", "instalação manutenção",
        "instalação manutenção avulso", "instalação de licenças", "instalação de novos serviços",
        "instalação e manutenção de software", "instalação novos serviços cftv contrato",
        "instalação para implantação", "instalação/manutenção de software",
        "po instalação de novos serviços", "plano de instalação termica", "remanejamento",
        "reposicionamento"
    },
    "Manutenção Remota": {
        "acesso remoto", "manutenção remota", "manutenção remota cftv avulso",
        "manutenção remota cftv contrato", "manutenção remota telefonia avulso",
        "manutenção remota telefonia contrato", "manutenção remota de clientes avulso",
        "manutenção remota de clientes de contrato"
    },
    "Levantamento": {
        "apoio técnico cftv contrato", "configuração sirene", "criar usuário",
        "criação de ádio para central tel", "exportação de imagens cliente de contrato",
        "laudo", "levantamento", "levantamento para implantação", "levantamento para manutenção",
        "levantamento para manutenção avulso", "levantamento para manutenção contrato seg.eletroni",
        "levantamento para manutenção contrato telefonia", "levantamento para manutenção de contrato",
        "telefone de emergência", "usuário sistema gitel"
    },
    "Outros": {
        "acompanhamento técnico", "analítico", "atividades extras", "cdm", "devolução",
        "entrega", "entrega de documentação", "entrega de material", "equipamento para teste",
        "manutenção chamado avulso", "manutenção de chamado contrato", "orçamento", "poc",
        "procedimento", "qdv", "retirada", "recusa de tarefa", "retirada de equipamento",
        "retirada de equipamento para manutenção", "reunião csn sirene", "termo de aceite",
        "teste", "treinamento"
    }
}

PLANO_PARA_CATEGORIA_MAP = {
    plano.lower(): categoria
    for categoria, planos in MAPEAMENTO_TAREFAS.items()
    for plano in planos
}

def get_grupo_tarefa(plano_tarefa_str):
    if not plano_tarefa_str:
        return 'Não Categorizado'
    plano_normalizado = plano_tarefa_str.strip().lower()
    return PLANO_PARA_CATEGORIA_MAP.get(plano_normalizado, 'Outros')

def get_grupo_local(local_str):
    if not local_str: return 'Outros'
    local_upper = local_str.upper()
    melhor_match, menor_indice = None, float('inf')
    for keyword in KEYWORDS_LOCAIS:
        indice = local_upper.find(keyword.upper())
        if indice != -1 and indice < menor_indice:
            menor_indice, melhor_match = indice, keyword.title()
    return melhor_match if melhor_match else 'Outros'

# SLA de Atendimento (Início da OS) - Gerdau
def _calculate_sla_for_group(base_queryset, ativo_code, sla_hours):
    query = base_queryset.filter(
        Local_Empresa__icontains='gerdau', tarefas__Ativo__icontains=ativo_code,
        Data_Criacao_OS__isnull=False, Data_Iniciou_OS__isnull=False
    ).exclude(tarefas__Tipo_de_Tarefa__icontains='Preventiva').distinct()
    query_with_duration = query.annotate(tempo_decorrido=F('Data_Iniciou_OS') - F('Data_Criacao_OS'))
    atendido = query_with_duration.filter(tempo_decorrido__lte=timedelta(hours=sla_hours)).count()
    nao_atendido = query_with_duration.filter(tempo_decorrido__gt=timedelta(hours=sla_hours)).count()
    return {'atendido': atendido, 'nao_atendido': nao_atendido}

# SLA de Resolução (Envio para Verificação) - Gerdau
def _calculate_resolution_sla_for_group(base_queryset, ativo_code, sla_hours):
    query = base_queryset.filter(
        Local_Empresa__icontains='gerdau', tarefas__Ativo__icontains=ativo_code,
        Data_Criacao_OS__isnull=False, Data_Enviado_Verificacao__isnull=False
    ).exclude(tarefas__Tipo_de_Tarefa__icontains='Preventiva').distinct()
    query_with_duration = query.annotate(tempo_decorrido=F('Data_Enviado_Verificacao') - F('Data_Criacao_OS'))
    atendido = query_with_duration.filter(tempo_decorrido__lte=timedelta(hours=sla_hours)).count()
    nao_atendido = query_with_duration.filter(tempo_decorrido__gt=timedelta(hours=sla_hours)).count()
    return {'atendido': atendido, 'nao_atendido': nao_atendido}

# SLA de Resolução - Park Shopping Canoas
def _calculate_parkshopping_sla(base_queryset, sla_hours):
    query = base_queryset.filter(
        Local_Empresa__icontains='PARK SHOPPING CANOAS',
        Data_Criacao_OS__isnull=False, Data_Enviado_Verificacao__isnull=False
    ).exclude(tarefas__Tipo_de_Tarefa__icontains='Preventiva').distinct()
    query_with_duration = query.annotate(tempo_decorrido=F('Data_Enviado_Verificacao') - F('Data_Criacao_OS'))
    atendido = query_with_duration.filter(tempo_decorrido__lte=timedelta(hours=sla_hours)).count()
    nao_atendido = query_with_duration.filter(tempo_decorrido__gt=timedelta(hours=sla_hours)).count()
    return {'atendido': atendido, 'nao_atendido': nao_atendido}

def _calculate_cpfl_sla(base_queryset, sla_hours):
    query = base_queryset.filter(
        Local_Empresa__icontains='CPFL',
        Data_Criacao_OS__isnull=False, Data_Enviado_Verificacao__isnull=False
    ).exclude(tarefas__Tipo_de_Tarefa__icontains='Preventiva').distinct()
    query_with_duration = query.annotate(tempo_decorrido=F('Data_Enviado_Verificacao') - F('Data_Criacao_OS'))
    atendido = query_with_duration.filter(tempo_decorrido__lte=timedelta(hours=sla_hours)).count()
    nao_atendido = query_with_duration.filter(tempo_decorrido__gt=timedelta(hours=sla_hours)).count()
    return {'atendido': atendido, 'nao_atendido': nao_atendido}


def dashboard_view(request):
    start_date_str = request.GET.get('start_date', '')
    end_date_str = request.GET.get('end_date', '')
    selected_local = request.GET.get('local_grupo', 'Todos')

    ordens_no_periodo = OrdemDeServico.objects.all()

    if start_date_str and end_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
            ordens_no_periodo = ordens_no_periodo.filter(Data_Criacao_OS__gte=start_date, Data_Criacao_OS__lte=end_date)
        except (ValueError, TypeError):
            pass

    if selected_local and selected_local != 'Todos':
        ids_para_filtrar = [os['id'] for os in OrdemDeServico.objects.exclude(Local_Empresa__isnull=True).values('id', 'Local_Empresa') if get_grupo_local(os['Local_Empresa']) == selected_local]
        ordens_no_periodo = ordens_no_periodo.filter(id__in=ids_para_filtrar)

    os_abertas_no_periodo = ordens_no_periodo.count()
    os_concluidas = ordens_no_periodo.filter(Status='Concluído').count()
    os_canceladas = ordens_no_periodo.filter(Status='Cancelado').count()
    os_em_processo = ordens_no_periodo.filter(Status='Em Processo').count()
    os_em_verificacao = ordens_no_periodo.filter(Status='Em Verificação').count()

    os_por_status = ordens_no_periodo.values('Status').annotate(total=Count('Status')).order_by('-total')
    os_por_criticidade = ordens_no_periodo.values('Nivel_de_Criticidade').annotate(total=Count('Nivel_de_Criticidade')).order_by('-total')
    os_por_ticket = ordens_no_periodo.exclude(Possui_Ticket__isnull=True).values('Possui_Ticket').annotate(total=Count('id')).order_by('Possui_Ticket')
    os_por_mes = (OrdemDeServico.objects.annotate(mes=TruncMonth('Data_Criacao_OS')).values('mes').annotate(total=Count('id')).order_by('mes'))

    os_ativas = ordens_no_periodo.exclude(Status__in=['Concluído', 'Cancelado'])
    ids_com_tarefa_in_progress = set(Tarefa.objects.filter(ordem_de_servico__in=os_ativas, Status_da_Tarefa='IN_PROGRESS').values_list('ordem_de_servico_id', flat=True))
    ids_com_avanco_e_processo = set(os_ativas.filter(Status='Em Processo', Avanco_da_OS__gt=0).values_list('id', flat=True))
    em_andamento_ids = ids_com_tarefa_in_progress.union(ids_com_avanco_e_processo)
    candidatas_pausadas_ids = set(Tarefa.objects.filter(ordem_de_servico__in=os_ativas, Status_da_Tarefa='PAUSED').values_list('ordem_de_servico_id', flat=True))
    ids_em_verificacao = set(os_ativas.filter(Status='Em Verificação').values_list('id', flat=True))
    pausadas_ids = candidatas_pausadas_ids - em_andamento_ids - ids_em_verificacao
    ids_ja_classificadas = em_andamento_ids.union(pausadas_ids)
    candidatas_restantes = os_ativas.exclude(id__in=ids_ja_classificadas)
    nao_iniciadas_ids = set(candidatas_restantes.filter(Q(Avanco_da_OS=0) | Q(Avanco_da_OS__isnull=True), Status='Em Processo').values_list('id', flat=True))
    em_andamento_count, pausadas_count, nao_iniciadas_count = len(em_andamento_ids), len(pausadas_ids), len(nao_iniciadas_ids)

    contagem_por_local = (ordens_no_periodo.exclude(Local_Empresa__isnull=True).exclude(Local_Empresa__exact='').values('Local_Empresa').annotate(total=Count('id')))
    grupos_finais = defaultdict(int)
    for item in contagem_por_local:
        grupo = get_grupo_local(item['Local_Empresa'])
        grupos_finais[grupo] += item['total']
    grupos_ordenados = sorted(grupos_finais.items(), key=lambda x: x[1], reverse=True)
    local_agrupado_labels = [item[0] for item in grupos_ordenados]
    local_agrupado_data = [item[1] for item in grupos_ordenados]

    tarefas_no_periodo = Tarefa.objects.filter(ordem_de_servico__in=ordens_no_periodo).exclude(Plano_de_Tarefas__isnull=True).exclude(Plano_de_Tarefas__exact='')
    contagem_grupo_tarefa = defaultdict(int)
    for tarefa in tarefas_no_periodo:
        grupo = get_grupo_tarefa(tarefa.Plano_de_Tarefas)
        contagem_grupo_tarefa[grupo] += 1
    tarefa_grupo_labels = list(contagem_grupo_tarefa.keys())
    tarefa_grupo_data = list(contagem_grupo_tarefa.values())



    sla_atendimento_c0 = _calculate_sla_for_group(ordens_no_periodo, '(C0)', 3)
    sla_atendimento_c1 = _calculate_sla_for_group(ordens_no_periodo, '(C1)', 12)
    sla_atendimento_c2 = _calculate_sla_for_group(ordens_no_periodo, '(C2)', 24)
    
    sla_resolucao_c0 = _calculate_resolution_sla_for_group(ordens_no_periodo, '(C0)', 8)
    sla_resolucao_c1 = _calculate_resolution_sla_for_group(ordens_no_periodo, '(C1)', 24)
    sla_resolucao_c2 = _calculate_resolution_sla_for_group(ordens_no_periodo, '(C2)', 48)

    sla_parkshopping_data = _calculate_parkshopping_sla(ordens_no_periodo, 24)

    sla_cpfl_data = _calculate_cpfl_sla(ordens_no_periodo, 72)

    anos_disponiveis = OrdemDeServico.objects.values_list('Ano_Criacao', flat=True).distinct().order_by('-Ano_Criacao')
    selected_year_weekly = request.GET.get('selected_year_weekly')
    weekly_labels, weekly_data_points = [], []
    if selected_year_weekly:
        try:
            selected_year_weekly = int(selected_year_weekly)
            weekly_counts_from_db = (OrdemDeServico.objects.filter(Ano_Criacao=selected_year_weekly).annotate(semana=ExtractWeek('Data_Criacao_OS')).values('semana').annotate(total=Count('id')).order_by('semana'))
            counts_dict = {item['semana']: item['total'] for item in weekly_counts_from_db}
            weekly_labels, weekly_data_points = [f"Sem {i}" for i in range(1, 54)], [counts_dict.get(i, 0) for i in range(1, 54)]
        except (ValueError, TypeError):
            selected_year_weekly = None

    grupos_de_local_disponiveis = ['Todos'] + sorted([kw.title() for kw in KEYWORDS_LOCAIS] + ['Outros'])

    context = {
        'os_abertas_no_periodo': os_abertas_no_periodo, 'os_concluidas': os_concluidas,
        'os_em_processo': os_em_processo,
        'os_em_verificacao': os_em_verificacao, 'os_canceladas': os_canceladas,
        'status_labels': [item['Status'] for item in os_por_status],
        'status_data': [item['total'] for item in os_por_status],
        'criticidade_labels': [item['Nivel_de_Criticidade'] for item in os_por_criticidade],
        'criticidade_data': [item['total'] for item in os_por_criticidade],
        'ticket_labels': [item['Possui_Ticket'] for item in os_por_ticket],
        'ticket_data': [item['total'] for item in os_por_ticket],
        'mes_labels': [d['mes'].strftime('%b/%Y') for d in os_por_mes if d['mes']],
        'mes_data': [d['total'] for d in os_por_mes if d['mes']],
        'execucao_status_labels': ['Em Andamento', 'Pausadas', 'Não Iniciadas'],
        'execucao_status_data': [em_andamento_count, pausadas_count, nao_iniciadas_count],
        'local_agrupado_labels': local_agrupado_labels, 'local_agrupado_data': local_agrupado_data,
        'start_date': start_date_str, 'end_date': end_date_str,
        'tarefa_grupo_labels': tarefa_grupo_labels,
        'tarefa_grupo_data': tarefa_grupo_data,
        'anos_disponiveis': anos_disponiveis, 'selected_year_weekly': selected_year_weekly,
        'weekly_labels': weekly_labels, 'weekly_data_points': weekly_data_points,
        'sla_atendimento_c0_data': sla_atendimento_c0, 'sla_atendimento_c1_data': sla_atendimento_c1, 'sla_atendimento_c2_data': sla_atendimento_c2,
        'sla_resolucao_c0_data': sla_resolucao_c0, 'sla_resolucao_c1_data': sla_resolucao_c1, 'sla_resolucao_c2_data': sla_resolucao_c2,
        'sla_parkshopping_data': sla_parkshopping_data,
        'sla_cpfl_data': sla_cpfl_data,
        'grupos_de_local_disponiveis': grupos_de_local_disponiveis,
        'selected_local': selected_local,
    }
    return render(request, 'Relatorio/dashboard.html', context)


def sla_violado_view(request, grupo_sla):
    grupo_sla = grupo_sla.upper()
    sla_map = {'C0': {'hours': 3, 'code': '(C0)'}, 'C1': {'hours': 12, 'code': '(C1)'},
               'C2': {'hours': 24, 'code': '(C2)'}, }
    if grupo_sla not in sla_map:
        return render(request, 'Relatorio/sla_violado.html', {'error': 'Grupo de SLA inválido.'})
    sla_info = sla_map[grupo_sla]
    os_violadas = (
        OrdemDeServico.objects.filter(
            Local_Empresa__icontains='gerdau', tarefas__Ativo__icontains=sla_info['code'],
            Data_Criacao_OS__isnull=False, Data_Iniciou_OS__isnull=False
        ).exclude(tarefas__Tipo_de_Tarefa__icontains='Preventiva').distinct().annotate(
            tempo_decorrido=F('Data_Iniciou_OS') - F('Data_Criacao_OS')).filter(
            tempo_decorrido__gt=timedelta(hours=sla_info['hours'])).order_by('-Data_Criacao_OS'))
    context = {'os_violadas': os_violadas, 'grupo_sla': grupo_sla, 'sla_hours': sla_info['hours'], 'tipo_sla': 'Atendimento'}
    return render(request, 'Relatorio/sla_violado.html', context)


def sla_resolucao_violado_view(request, grupo_sla):
    grupo_sla = grupo_sla.upper()
    sla_map = {
        'C0': {'hours': 8, 'code': '(C0)'},
        'C1': {'hours': 24, 'code': '(C1)'},
        'C2': {'hours': 48, 'code': '(C2)'},
    }
    if grupo_sla not in sla_map:
        return render(request, 'Relatorio/sla_violado.html', {'error': 'Grupo de SLA inválido.'})
    
    sla_info = sla_map[grupo_sla]
    os_violadas = (
        OrdemDeServico.objects.filter(
            Local_Empresa__icontains='gerdau', 
            tarefas__Ativo__icontains=sla_info['code'],
            Data_Criacao_OS__isnull=False, 
            Data_Enviado_Verificacao__isnull=False
        ).exclude(tarefas__Tipo_de_Tarefa__icontains='Preventiva').distinct().annotate(
            tempo_decorrido=F('Data_Enviado_Verificacao') - F('Data_Criacao_OS')
        ).filter(
            tempo_decorrido__gt=timedelta(hours=sla_info['hours'])
        ).order_by('-Data_Criacao_OS')
    )
    context = {
        'os_violadas': os_violadas, 
        'grupo_sla': grupo_sla, 
        'sla_hours': sla_info['hours'],
        'tipo_sla': 'Resolução'
    }
    return render(request, 'Relatorio/sla_violado.html', context)


def sla_parkshopping_violado_view(request):
    sla_hours = 24
    os_violadas = (
        OrdemDeServico.objects.filter(
            Local_Empresa__icontains='PARK SHOPPING CANOAS',
            Data_Criacao_OS__isnull=False, 
            Data_Enviado_Verificacao__isnull=False
        ).exclude(tarefas__Tipo_de_Tarefa__icontains='Preventiva').distinct().annotate(
            tempo_decorrido=F('Data_Enviado_Verificacao') - F('Data_Criacao_OS')
        ).filter(
            tempo_decorrido__gt=timedelta(hours=sla_hours)
        ).order_by('-Data_Criacao_OS')
    )
    context = {
        'os_violadas': os_violadas, 
        'sla_hours': sla_hours,
        'tipo_sla': 'Resolução' # Corrigido para maior clareza
    }
    return render(request, 'Relatorio/sla_violado.html', context)

def sla_cpfl_violado_view(request):
    sla_hours = 72
    os_violadas = (
        OrdemDeServico.objects.filter(
            Local_Empresa__icontains='CPFL',
            Data_Criacao_OS__isnull=False,
            Data_Enviado_Verificacao__isnull=False
        ).exclude(tarefas__Tipo_de_Tarefa__icontains='Preventiva').distinct().annotate(
            tempo_decorrido=F('Data_Enviado_Verificacao') - F('Data_Criacao_OS')
        ).filter(
            tempo_decorrido__gt=timedelta(hours=sla_hours)
        ).order_by('-Data_Criacao_OS')
    )
    context = {
        'os_violadas': os_violadas,
        'sla_hours': sla_hours,
        'tipo_sla': 'Resolução'
    }
    return render(request, 'Relatorio/sla_violado.html', context)


def _get_dados_filtrados(tipo_relatorio, categoria, start_date_str, end_date_str):
    base_queryset = OrdemDeServico.objects.all()

    if start_date_str and end_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
            base_queryset = base_queryset.filter(Data_Criacao_OS__gte=start_date, Data_Criacao_OS__lte=end_date)
        except (ValueError, TypeError):
            pass

    if categoria and categoria != 'todas':
        if tipo_relatorio == 'status':
            base_queryset = base_queryset.filter(Status=categoria)
            if categoria == 'Em Processo':
                base_queryset = base_queryset.annotate(tempo_em_status=Now() - F('Data_Criacao_OS'))
            elif categoria == 'Em Verificação':
                base_queryset = base_queryset.annotate(tempo_em_status=Now() - F('Data_Enviado_Verificacao'))
            elif categoria == 'Concluído':
                base_queryset = base_queryset.annotate(
                    tempo_em_execucao=F('Data_Enviado_Verificacao') - F('Data_Criacao_OS'),
                    tempo_em_verificacao=F('Data_Finalizacao_OS') - F('Data_Enviado_Verificacao')
                )

        elif tipo_relatorio == 'criticidade':
            base_queryset = base_queryset.filter(Nivel_de_Criticidade=categoria)
        elif tipo_relatorio == 'ticket':
            base_queryset = base_queryset.filter(Possui_Ticket=categoria)
        elif tipo_relatorio == 'execucao':
            os_ativas = base_queryset.exclude(Status__in=['Concluído', 'Cancelado'])
            ids_com_tarefa_in_progress = set(Tarefa.objects.filter(ordem_de_servico__in=os_ativas, Status_da_Tarefa='IN_PROGRESS').values_list('ordem_de_servico_id', flat=True))
            ids_com_avanco_e_processo = set(os_ativas.filter(Status='Em Processo', Avanco_da_OS__gt=0).values_list('id', flat=True))
            em_andamento_ids = ids_com_tarefa_in_progress.union(ids_com_avanco_e_processo)
            candidatas_pausadas_ids = set(Tarefa.objects.filter(ordem_de_servico__in=os_ativas, Status_da_Tarefa='PAUSED').values_list('ordem_de_servico_id', flat=True))
            ids_em_verificacao = set(os_ativas.filter(Status='Em Verificação').values_list('id', flat=True))
            pausadas_ids = candidatas_pausadas_ids - em_andamento_ids - ids_em_verificacao
            ids_ja_classificadas = em_andamento_ids.union(pausadas_ids)
            candidatas_restantes = os_ativas.exclude(id__in=ids_ja_classificadas)
            nao_iniciadas_ids = set(candidatas_restantes.filter(Q(Avanco_da_OS=0) | Q(Avanco_da_OS__isnull=True), Status='Em Processo').values_list('id', flat=True))

            if categoria == 'Em Andamento': base_queryset = base_queryset.filter(id__in=em_andamento_ids)
            elif categoria == 'Pausadas': base_queryset = base_queryset.filter(id__in=pausadas_ids)
            elif categoria == 'Não Iniciadas': base_queryset = base_queryset.filter(id__in=nao_iniciadas_ids)

        elif tipo_relatorio == 'locais_agrupados':
            ids_para_filtrar = [os['id'] for os in base_queryset.exclude(Local_Empresa__isnull=True).values('id', 'Local_Empresa') if get_grupo_local(os['Local_Empresa']) == categoria]
            base_queryset = OrdemDeServico.objects.filter(id__in=ids_para_filtrar)

    return base_queryset.order_by('-Data_Criacao_OS')


def extracao_view(request):
    if request.method == 'POST':
        tipo_relatorio = request.POST.get('tipo_relatorio', '')
        categoria = request.POST.get('categoria', '')
        start_date = request.POST.get('start_date', '')
        end_date = request.POST.get('end_date', '')
        formato_saida = request.POST.get('formato_saida', '')
        params = f"?tipo_relatorio={tipo_relatorio}&categoria={categoria}&start_date={start_date}&end_date={end_date}"
        if formato_saida == 'excel':
            return redirect(reverse('download_excel') + params)
        else:
            return redirect(reverse('resultado_relatorio') + params)
    return render(request, 'Relatorio/extracao.html')


def resultado_view(request):
    tipo_relatorio = request.GET.get('tipo_relatorio')
    categoria = request.GET.get('categoria')
    start_date = request.GET.get('start_date')
    end_date = request.GET.get('end_date')

    dados_filtrados = _get_dados_filtrados(tipo_relatorio, categoria, start_date, end_date)
    mostrar_tempo_em_status = (tipo_relatorio == 'status' and categoria in ['Em Processo', 'Em Verificação'])
    mostrar_tempos_conclusao = (tipo_relatorio == 'status' and categoria == 'Concluído')
    nome_coluna_tempo = f"Tempo {categoria}" if mostrar_tempo_em_status else ""

    context = {
        'ordens_de_servico': dados_filtrados,
        'total': dados_filtrados.count(),
        'tipo_relatorio': tipo_relatorio,
        'tipo_relatorio_display': tipo_relatorio.replace("_", " ").title() if tipo_relatorio else '',
        'categoria_display': categoria if categoria != 'todas' else 'Todas as Categorias',
        'start_date': start_date,
        'end_date': end_date,
        'mostrar_tempo_em_status': mostrar_tempo_em_status,
        'mostrar_tempos_conclusao': mostrar_tempos_conclusao,
        'nome_coluna_tempo': nome_coluna_tempo,
    }
    return render(request, 'Relatorio/resultado_extracao.html', context)


def gerar_excel_view(request):
    tipo_relatorio = request.GET.get('tipo_relatorio')
    categoria = request.GET.get('categoria')
    start_date = request.GET.get('start_date')
    end_date = request.GET.get('end_date')

    dados_filtrados = _get_dados_filtrados(tipo_relatorio, categoria, start_date, end_date)

    colunas_excel = [
        'OS', 'Status', 'Nivel_de_Criticidade', 'Local_Empresa', 'Criado_Por',
        'Data_Criacao_OS', 'Data_Iniciou_OS', 'Data_Finalizacao_OS',
        'Avanco_da_OS', 'Possui_Ticket', 'Ticket_ID', 'Observacao_OS'
    ]

    if tipo_relatorio == 'status' and categoria in ['Em Processo', 'Em Verificação']:
        colunas_excel.append('tempo_em_status')
    elif tipo_relatorio == 'status' and categoria == 'Concluído':
        colunas_excel.extend(['tempo_em_execucao', 'tempo_em_verificacao'])

    dados_para_excel = dados_filtrados.values(*colunas_excel)
    df = pd.DataFrame(list(dados_para_excel))

    colunas_de_data = ['Data_Criacao_OS', 'Data_Iniciou_OS', 'Data_Finalizacao_OS']
    for coluna in colunas_de_data:
        if coluna in df.columns:
            df[coluna] = df[coluna].apply(lambda x: x.tz_localize(None) if pd.notnull(x) else x)

    def format_excel_timedelta(td):
        if pd.isnull(td) or not isinstance(td, timedelta): return ''
        total_seconds = int(td.total_seconds())
        days, remainder = divmod(total_seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)
        if days > 0: return f'{days}d {hours:02}:{minutes:02}:{seconds:02}'
        return f'{hours:02}:{minutes:02}:{seconds:02}'

    for col in ['tempo_em_status', 'tempo_em_execucao', 'tempo_em_verificacao']:
        if col in df.columns:
            df[col] = df[col].apply(format_excel_timedelta)

    rename_dict = {
        'OS': 'OS', 'Status': 'Status', 'Nivel_de_Criticidade': 'Criticidade',
        'Local_Empresa': 'Local/Empresa', 'Criado_Por': 'Criado Por',
        'Data_Criacao_OS': 'Data Criação', 'Data_Iniciou_OS': 'Data Início',
        'Data_Finalizacao_OS': 'Data Finalização', 'Avanco_da_OS': 'Avanço (%)',
        'Possui_Ticket': 'Possui Ticket?', 'Ticket_ID': 'ID do Ticket',
        'Observacao_OS': 'Observação',
        'tempo_em_status': f"Tempo em {categoria}",
        'tempo_em_execucao': "Tempo em Execução",
        'tempo_em_verificacao': "Tempo em Verificação"
    }
    df.rename(columns=rename_dict, inplace=True)

    for col in ['Data Criação', 'Data Início', 'Data Finalização']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Relatorio')
    buffer.seek(0)
    response = HttpResponse(
        buffer, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response[
        'Content-Disposition'] = f'attachment; filename="relatorio_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx"'
    return response


def api_get_categorias_view(request):
    tipo_relatorio = request.GET.get('tipo_relatorio')
    categorias = ['todas']

    if tipo_relatorio == 'status':
        qs = OrdemDeServico.objects.exclude(Status__isnull=True).exclude(Status='').values_list('Status', flat=True).distinct()
        categorias.extend(list(qs))
    elif tipo_relatorio == 'criticidade':
        qs = OrdemDeServico.objects.exclude(Nivel_de_Criticidade__isnull=True).exclude(
            Nivel_de_Criticidade='').values_list('Nivel_de_Criticidade', flat=True).distinct()
        categorias.extend(list(qs))
    elif tipo_relatorio == 'ticket':
        categorias.extend(['Sim', 'Não'])
    elif tipo_relatorio == 'execucao':
        categorias.extend(['Em Andamento', 'Pausadas', 'Não Iniciadas'])
    elif tipo_relatorio == 'locais_agrupados':
        categorias.extend(sorted([kw.title() for kw in KEYWORDS_LOCAIS]))
        categorias.append('Outros')

    return JsonResponse({'categorias': categorias})