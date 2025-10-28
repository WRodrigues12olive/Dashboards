from django.shortcuts import render
from django.db.models import Count, F, Q
from django.db.models.functions import TruncMonth, ExtractWeek, Now
from django.db.models import Avg, ExpressionWrapper, DurationField
from dateutil.relativedelta import relativedelta
from .mappings import KEYWORDS_LOCAIS, MAPEAMENTO_PLANO_TAREFAS_DETALHADO, MAPEAMENTO_TECNICOS
from .models import OrdemDeServico, Tarefa
from datetime import datetime, timedelta
from django.http import JsonResponse, HttpResponse
from django.shortcuts import redirect
from django.urls import reverse
import pandas as pd
import io
from collections import defaultdict
import unicodedata
import re
import difflib


def resumo_clientes_view(request):
    # Lista de clientes específicos (baseado nas keywords, ajuste os nomes se necessário)
    clientes_target_keywords = [
        "GERDAU", "LOJA MAÇONICA", "HOSPITAL DE CLINICAS", "PARK SHOPPING CANOAS",
        "TRT", # Assumindo que TRT4 é o keyword para TRT
        "UNILEVER",
        "FUNDAÇÃO BANRISUL", # Keyword para Banrisul
        "CSN", "CPFL", "ALPHAVILLE", "ASSEMBLEIA"
    ]
    # Mapeamento de keyword para nome de exibição (opcional, para nomes mais amigáveis)
    cliente_display_names = {
        "GERDAU": "Gerdau",
        "LOJA MAÇONICA": "Loja Maçônica",
        "HOSPITAL DE CLINICAS": "Hospital de Clínicas",
        "PARK SHOPPING CANOAS": "Park Shopping Canoas",
        "TRT": "TRT",
        "UNILEVER": "Unilever",
        "FUNDAÇÃO BANRISUL": "FUNDAÇÃO BANRISUL",
        "CSN": "CSN",
        "CPFL": "CPFL",
        "ALPHAVILLE": "Alphaville",
        "ASSEMBLEIA": "Assembleia Legislativa" # Exemplo de nome mais completo
    }


    # --- Tratamento das Datas ---
    today = datetime.now().date()
    # Data padrão: 6 meses atrás (primeiro dia daquele mês)
    default_start_date = (today - relativedelta(months=6)).replace(day=1)
    default_end_date = today

    # Obtém datas do GET ou usa os padrões
    start_date_str = request.GET.get('start_date', default_start_date.strftime('%Y-%m-%d'))
    end_date_str = request.GET.get('end_date', default_end_date.strftime('%Y-%m-%d'))

    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        # Adiciona 1 dia ao end_date para incluir o dia todo na query (__lt)
        # Ou ajusta para o fim do dia se preferir usar __lte
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        end_date_query = end_date + timedelta(days=1) # Para usar com __lt
        # Ou: end_date_query = datetime.combine(end_date, datetime.max.time()) # Para usar com __lte
    except (ValueError, TypeError):
        start_date = default_start_date
        end_date = default_end_date
        end_date_query = end_date + timedelta(days=1)
        # Ou: end_date_query = datetime.combine(end_date, datetime.max.time())

    os_no_periodo_filtrado = OrdemDeServico.objects.filter(
        Data_Criacao_OS__gte=start_date,
        Data_Criacao_OS__lt=end_date_query
    )

    os_por_mes = (
        OrdemDeServico.objects  # Começa com todas as OS
        .annotate(mes=TruncMonth('Data_Criacao_OS'))  # Agrupa por mês
        .values('mes')  # Seleciona o mês
        .annotate(total=Count('id'))  # Conta OSs por mês
        .order_by('mes')  # Ordena pelo mês
    )  #
    mes_labels = [d['mes'].strftime('%b/%Y') for d in os_por_mes if d['mes']]  # Formata 'Mês/Ano' #
    mes_data = [d['total'] for d in os_por_mes if d['mes']]

    # --- Cálculo dos Dados por Cliente ---
    data_resumo = []

    for keyword in clientes_target_keywords:
        # Filtra OSs do cliente no período
        os_cliente_periodo = OrdemDeServico.objects.filter(
            Local_Empresa__icontains=keyword,
            Data_Criacao_OS__gte=start_date,
            Data_Criacao_OS__lt=end_date_query # Usa __lt com o dia seguinte
            # Ou: Data_Criacao_OS__lte=end_date_query # Se usou datetime.combine
        )

        total_os = os_cliente_periodo.count()
        if total_os == 0: # Pula se não houver OS para o cliente no período
             # Ou adiciona uma linha com zeros,
             # data_resumo.append({
             #     'cliente': cliente_display_names.get(keyword, keyword),
             #     'total_os': 0, 'concluidas': 0, 'em_processo': 0,
             #     'em_verificacao': 0, 'canceladas': 0,
             #     'tempo_medio_atendimento': None, 'sla_status': 'N/A'
             # })
             continue

        concluidas = os_cliente_periodo.filter(Status='Concluído').count()
        em_processo = os_cliente_periodo.filter(Status='Em Processo').count()
        em_verificacao = os_cliente_periodo.filter(Status='Em Verificação').count()
        canceladas = os_cliente_periodo.filter(Status='Cancelado').count()

        tempo_medio_atendimento = os_cliente_periodo.filter(
            Data_Enviado_Verificacao__isnull=False,
            Data_Criacao_OS__isnull=False
        ).aggregate(
            avg_duration=Avg(
                ExpressionWrapper(F('Data_Enviado_Verificacao') - F('Data_Criacao_OS'), output_field=DurationField())
            )
        )['avg_duration']

        data_resumo.append({
            'cliente': cliente_display_names.get(keyword, keyword),
            'total_os': total_os,
            'em_processo': em_processo,
            'em_verificacao': em_verificacao,
            'concluidas': concluidas,
            'canceladas': canceladas,
            'tempo_medio_atendimento': tempo_medio_atendimento,
            'sla_status': 'N/A'
        })

    os_por_ticket_periodo = os_no_periodo_filtrado.exclude(
        Possui_Ticket__isnull=True
    ).values('Possui_Ticket').annotate(total=Count('id')).order_by('Possui_Ticket')  #
    ticket_labels_periodo = [item['Possui_Ticket'] for item in os_por_ticket_periodo]

    ticket_labels_periodo = []
    for item in os_por_ticket_periodo:
        label = item['Possui_Ticket']
        if label == 'Sim':
            ticket_labels_periodo.append('Com Ticket')
        elif label == 'Não':
            ticket_labels_periodo.append('Sem Ticket')
        else:
            ticket_labels_periodo.append(label)

    ticket_data_periodo = [item['total'] for item in os_por_ticket_periodo]  #

    tarefas_tipos_no_periodo_filtrado = Tarefa.objects.filter(
        ordem_de_servico__in=os_no_periodo_filtrado
    ).exclude(Tipo_de_Tarefa__isnull=True).exclude(Tipo_de_Tarefa__exact=''
                                                   ).values('ordem_de_servico_id', 'Tipo_de_Tarefa')  #

    grupos_tipo_por_os_periodo = defaultdict(set)
    for tarefa_data in tarefas_tipos_no_periodo_filtrado:
        os_id = tarefa_data['ordem_de_servico_id']  #
        tipo_tarefa = tarefa_data['Tipo_de_Tarefa']  #
        grupo_tipo = get_grupo_tipo_tarefa(tipo_tarefa)  #
        grupos_tipo_por_os_periodo[os_id].add(grupo_tipo)  #

    contagem_grupo_tipo_tarefa_periodo = defaultdict(int)
    for os_id, grupos_da_os in grupos_tipo_por_os_periodo.items():
        for grupo in grupos_da_os:  #
            contagem_grupo_tipo_tarefa_periodo[grupo] += 1  #

    tipo_tarefa_grupo_labels_periodo = list(contagem_grupo_tipo_tarefa_periodo.keys())  #
    tipo_tarefa_grupo_data_periodo = list(contagem_grupo_tipo_tarefa_periodo.values())

    statuses_interesse_tecnico = ['Em Processo', 'Em Verificação', 'Concluído']
    contagem_tecnico_status_resumo = defaultdict(lambda: defaultdict(int))

    os_ids_filtrados = os_no_periodo_filtrado.values_list('id', flat=True)

    tarefas_tecnicos_resumo = Tarefa.objects.filter(
        ordem_de_servico_id__in=os_ids_filtrados,
        ordem_de_servico__Status__in=statuses_interesse_tecnico
    ).exclude(
        Responsavel__isnull=True
    ).exclude(
        Responsavel__exact=''
    ).select_related('ordem_de_servico').values(
        'ordem_de_servico_id',
        'ordem_de_servico__Status',
        'Responsavel'
    )

    os_tecnicos_map_resumo = defaultdict(set)
    os_status_map_resumo = {}
    for tarefa in tarefas_tecnicos_resumo:
        os_id = tarefa['ordem_de_servico_id']
        grupo_tecnico = get_grupo_tecnico(tarefa['Responsavel'])
        if grupo_tecnico != 'Não Mapeado' and grupo_tecnico != 'Outros':
            os_tecnicos_map_resumo[os_id].add(grupo_tecnico)
        os_status_map_resumo[os_id] = tarefa['ordem_de_servico__Status']

    for os_id, tecnicos_da_os in os_tecnicos_map_resumo.items():
        status_da_os = os_status_map_resumo.get(os_id)
        if status_da_os:
            for tecnico in tecnicos_da_os:
                contagem_tecnico_status_resumo[tecnico][status_da_os] += 1

    tecnicos_ordenados_status_resumo = sorted(
        contagem_tecnico_status_resumo.items(),
        key=lambda item: sum(item[1].values()),
        reverse=True
    )

    tecnico_status_labels_resumo = [item[0] for item in tecnicos_ordenados_status_resumo]
    tecnico_status_data_processo_resumo = [item[1].get('Em Processo', 0) for item in tecnicos_ordenados_status_resumo]
    tecnico_status_data_verificacao_resumo = [item[1].get('Em Verificação', 0) for item in
                                              tecnicos_ordenados_status_resumo]
    tecnico_status_data_concluido_resumo = [item[1].get('Concluído', 0) for item in tecnicos_ordenados_status_resumo]

    context = {
        'data_resumo': data_resumo,
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
        'mes_labels': mes_labels,
        'mes_data': mes_data,
        'ticket_labels_periodo': ticket_labels_periodo,
        'ticket_data_periodo': ticket_data_periodo,
        'tipo_tarefa_grupo_labels_periodo': tipo_tarefa_grupo_labels_periodo,
        'tipo_tarefa_grupo_data_periodo': tipo_tarefa_grupo_data_periodo,
        'tecnico_status_labels_resumo': tecnico_status_labels_resumo,
        'tecnico_status_data_processo_resumo': tecnico_status_data_processo_resumo,
        'tecnico_status_data_verificacao_resumo': tecnico_status_data_verificacao_resumo,
        'tecnico_status_data_concluido_resumo': tecnico_status_data_concluido_resumo,
    }

    return render(request, 'Relatorio/resumo_clientes.html', context)



def normalize_text(s: str) -> str:
    if not s:
        return ''
    s = unicodedata.normalize('NFKD', s)
    s = s.encode('ASCII', 'ignore').decode('utf-8')
    s = s.lower()
    s = re.sub(r'\s+', ' ', s).strip()
    s = re.sub(r'[^0-9a-z\-\s/]', '', s)
    return s

TECNICO_PARA_GRUPO_MAP = {}
for grupo_principal, nomes_brutos in MAPEAMENTO_TECNICOS.items():
    for nome_bruto in nomes_brutos:
        key = normalize_text(nome_bruto)
        TECNICO_PARA_GRUPO_MAP[key] = grupo_principal

KNOWN_TECNICO_KEYS = list(TECNICO_PARA_GRUPO_MAP.keys())


TIPO_TAREFA_PARA_GRUPO = {}
for grupo, tipos in MAPEAMENTO_PLANO_TAREFAS_DETALHADO.items():
    for tipo in tipos:
        tipo_normalizado = ' '.join(tipo.strip().split()).lower()
        TIPO_TAREFA_PARA_GRUPO[tipo_normalizado] = grupo


def get_grupo_tecnico(responsavel_str):
    if not responsavel_str:
        return 'Não Mapeado'
    responsavel_normalizado = normalize_text(responsavel_str)

    grupo = TECNICO_PARA_GRUPO_MAP.get(responsavel_normalizado)
    if grupo:
        return grupo

    for key in KNOWN_TECNICO_KEYS:
        if key and (key in responsavel_normalizado or responsavel_normalizado in key):
            return TECNICO_PARA_GRUPO_MAP[key]

    close = difflib.get_close_matches(responsavel_normalizado, KNOWN_TECNICO_KEYS, n=1, cutoff=0.85)
    if close:
        return TECNICO_PARA_GRUPO_MAP[close[0]]

    return 'Outros'

def get_grupo_tipo_tarefa(tipo_tarefa_str):
    if not tipo_tarefa_str:
        return 'Não Categorizado'
    tipo_normalizado = ' '.join(tipo_tarefa_str.strip().split()).lower()
    return TIPO_TAREFA_PARA_GRUPO.get(tipo_normalizado, 'Outros')

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

    tarefas_tipos_no_periodo = Tarefa.objects.filter(
        ordem_de_servico__in=ordens_no_periodo
    ).exclude(Tipo_de_Tarefa__isnull=True).exclude(Tipo_de_Tarefa__exact=''
                                                   ).values(
        'ordem_de_servico_id',
        'Tipo_de_Tarefa',
    )

    grupos_tipo_por_os = defaultdict(set)

    for tarefa_data in tarefas_tipos_no_periodo:
        os_id = tarefa_data['ordem_de_servico_id']
        tipo_tarefa = tarefa_data['Tipo_de_Tarefa']
        grupo_tipo = get_grupo_tipo_tarefa(tipo_tarefa)
        grupos_tipo_por_os[os_id].add(grupo_tipo)

    contagem_grupo_tipo_tarefa = defaultdict(int)
    for os_id, grupos_da_os in grupos_tipo_por_os.items():
        for grupo in grupos_da_os:
            contagem_grupo_tipo_tarefa[grupo] += 1

    tipo_tarefa_grupo_labels = list(contagem_grupo_tipo_tarefa.keys())
    tipo_tarefa_grupo_data = list(contagem_grupo_tipo_tarefa.values())

    tarefas_tecnicos_no_periodo = Tarefa.objects.filter(
        ordem_de_servico__in=ordens_no_periodo
    ).exclude(Responsavel__isnull=True).exclude(Responsavel__exact=''
                                                ).values(
        'ordem_de_servico_id',
        'Responsavel'
    )

    grupos_tecnico_por_os = defaultdict(set)

    for tarefa_data in tarefas_tecnicos_no_periodo:
        os_id = tarefa_data['ordem_de_servico_id']
        responsavel = tarefa_data['Responsavel']
        grupo_tecnico = get_grupo_tecnico(responsavel)
        grupos_tecnico_por_os[os_id].add(grupo_tecnico)

    contagem_grupo_tecnico = defaultdict(int)
    for os_id, grupos_da_os in grupos_tecnico_por_os.items():
        for grupo in grupos_da_os:
            contagem_grupo_tecnico[grupo] += 1

    grupos_tecnico_ordenados = sorted(contagem_grupo_tecnico.items(), key=lambda item: item[1], reverse=True)

    tecnico_grupo_labels = [item[0] for item in grupos_tecnico_ordenados]
    tecnico_grupo_data = [item[1] for item in grupos_tecnico_ordenados]

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
        'tipo_tarefa_grupo_labels': tipo_tarefa_grupo_labels,
        'tipo_tarefa_grupo_data': tipo_tarefa_grupo_data,
        'anos_disponiveis': anos_disponiveis, 'selected_year_weekly': selected_year_weekly,
        'weekly_labels': weekly_labels, 'weekly_data_points': weekly_data_points,
        'sla_atendimento_c0_data': sla_atendimento_c0, 'sla_atendimento_c1_data': sla_atendimento_c1, 'sla_atendimento_c2_data': sla_atendimento_c2,
        'sla_resolucao_c0_data': sla_resolucao_c0, 'sla_resolucao_c1_data': sla_resolucao_c1, 'sla_resolucao_c2_data': sla_resolucao_c2,
        'sla_parkshopping_data': sla_parkshopping_data,
        'sla_cpfl_data': sla_cpfl_data,
        'grupos_de_local_disponiveis': grupos_de_local_disponiveis,
        'selected_local': selected_local,
        'tecnico_grupo_labels': tecnico_grupo_labels,
        'tecnico_grupo_data': tecnico_grupo_data,
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
        'tipo_sla': 'Resolução'
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

    base_queryset = base_queryset.annotate(
        tempo_em_execucao=F('Data_Enviado_Verificacao') - F('Data_Criacao_OS'),
        tempo_em_verificacao=F('Data_Finalizacao_OS') - F('Data_Enviado_Verificacao')
    )

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


        elif tipo_relatorio == 'tipo_tarefa_agrupados':
            tarefas_no_periodo = Tarefa.objects.filter(
                ordem_de_servico__in=base_queryset
            ).exclude(Tipo_de_Tarefa__isnull=True).exclude(Tipo_de_Tarefa__exact='')

            ids_os_para_filtrar = set()

            for tarefa in tarefas_no_periodo:
                if get_grupo_tipo_tarefa(tarefa.Tipo_de_Tarefa) == categoria:
                    ids_os_para_filtrar.add(tarefa.ordem_de_servico_id)

            base_queryset = base_queryset.filter(id__in=list(ids_os_para_filtrar))

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
            base_queryset = base_queryset.filter(id__in=ids_para_filtrar)

        elif tipo_relatorio == 'tecnico_agrupados':
            tarefas_no_periodo = Tarefa.objects.filter(
                ordem_de_servico__in=base_queryset
            ).exclude(Responsavel__isnull=True).exclude(Responsavel__exact='')

            ids_os_para_filtrar = set()

            for tarefa in tarefas_no_periodo:
                if get_grupo_tecnico(tarefa.Responsavel) == categoria:
                    ids_os_para_filtrar.add(tarefa.ordem_de_servico_id)

            base_queryset = base_queryset.filter(id__in=list(ids_os_para_filtrar))


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
    mostrar_tempos_conclusao = tipo_relatorio in ['status', 'locais_agrupados', 'plano_tarefas_agrupados', 'tecnico_agrupados']
    nome_coluna_tempo = f"Tempo {categoria}" if mostrar_tempo_em_status else ""

    tipo_relatorio_display = ''
    if tipo_relatorio:
        if tipo_relatorio == 'plano_tarefas_agrupados':
            tipo_relatorio_display = 'Ocorrências por Grupo de Plano de Tarefas'

        elif tipo_relatorio == 'tecnico_agrupados':
            tipo_relatorio_display = 'Ocorrências por Grupo de Técnico'

        else:
            tipo_relatorio_display = tipo_relatorio.replace("_", " ").title()

    context = {
        'ordens_de_servico': dados_filtrados,
        'total': dados_filtrados.count(),
        'tipo_relatorio': tipo_relatorio,
        'tipo_relatorio_display': tipo_relatorio_display,
        'categoria_display': categoria if categoria != 'todas' else 'Todas as Categorias',
        'start_date': start_date,
        'end_date': end_date,
        'mostrar_tempo_em_status': mostrar_tempo_em_status,
        'mostrar_tempos_conclusao': mostrar_tempos_conclusao,
        'nome_coluna_tempo': nome_coluna_tempo,
    }
    return render(request, 'Relatorio/resultado_extracao.html', context)

def format_excel_timedelta(td):
    if pd.isnull(td) or not isinstance(td, timedelta): return ''
    total_seconds = int(td.total_seconds())
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    if days > 0: return f'{days}d {hours:02}:{minutes:02}:{seconds:02}'
    return f'{hours:02}:{minutes:02}:{seconds:02}'

def gerar_excel_view(request):
    tipo_relatorio = request.GET.get('tipo_relatorio')
    categoria = request.GET.get('categoria')
    start_date = request.GET.get('start_date')
    end_date = request.GET.get('end_date')

    # Chama _get_dados_filtrados (que já inclui as anotações de tempo)
    dados_filtrados = _get_dados_filtrados(tipo_relatorio, categoria, start_date, end_date)

    # Lista base de colunas
    colunas_excel = [
        'OS', 'Status', 'Nivel_de_Criticidade', 'Local_Empresa', 'Criado_Por',
        'Data_Criacao_OS', 'Data_Iniciou_OS', 'Data_Finalizacao_OS',
        'Avanco_da_OS', 'Possui_Ticket', 'Ticket_ID', 'Observacao_OS'
    ]

    # Adiciona colunas de tempo condicionalmente
    tipos_para_incluir_tempo = ['status', 'locais_agrupados', 'plano_tarefas_agrupados']

    if tipo_relatorio == 'status' and categoria in ['Em Processo', 'Em Verificação']:
        colunas_excel.append('tempo_em_status')
    elif tipo_relatorio in tipos_para_incluir_tempo:
        # Inclui os tempos de execução e verificação para os tipos selecionados
        colunas_excel.extend(['tempo_em_execucao', 'tempo_em_verificacao'])

    # Pega os valores do queryset (incluindo as anotações, se existirem)
    dados_para_excel = dados_filtrados.values(*colunas_excel)
    df = pd.DataFrame(list(dados_para_excel))

    # Formata colunas de data
    colunas_de_data = ['Data_Criacao_OS', 'Data_Iniciou_OS', 'Data_Finalizacao_OS']
    for coluna in colunas_de_data:
        if coluna in df.columns:
             df[coluna] = df[coluna].apply(lambda x: x.tz_localize(None) if pd.notnull(x) else x)

    # Formata colunas de tempo (timedelta)
    for col in ['tempo_em_status', 'tempo_em_execucao', 'tempo_em_verificacao']:
        if col in df.columns:
            df[col] = df[col].apply(format_excel_timedelta)

    # Dicionário base para renomear colunas
    rename_dict = {
        'OS': 'OS', 'Status': 'Status', 'Nivel_de_Criticidade': 'Criticidade',
        'Local_Empresa': 'Local/Empresa', 'Criado_Por': 'Criado Por',
        'Data_Criacao_OS': 'Data Criação', 'Data_Iniciou_OS': 'Data Início',
        'Data_Finalizacao_OS': 'Data Finalização', 'Avanco_da_OS': 'Avanço (%)',
        'Possui_Ticket': 'Possui Ticket?', 'Ticket_ID': 'ID do Ticket',
        'Observacao_OS': 'Observação',
    }
    # Adiciona renomeação condicional para colunas de tempo
    if tipo_relatorio == 'status' and categoria in ['Em Processo', 'Em Verificação']:
         rename_dict['tempo_em_status'] = f"Tempo em {categoria}"
    elif tipo_relatorio in tipos_para_incluir_tempo:
         rename_dict['tempo_em_execucao'] = "Tempo em Execução"
         rename_dict['tempo_em_verificacao'] = "Tempo em Verificação"

    df.rename(columns=rename_dict, inplace=True)

    # Formata colunas de data como string no formato desejado
    for col in ['Data Criação', 'Data Início', 'Data Finalização']:
        if col in df.columns:
            # Converte para datetime (se ainda não for), tratando erros, e depois formata
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

    # Gera o arquivo Excel em memória
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Relatorio')
    buffer.seek(0)

    # Cria a resposta HTTP para download
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
    elif tipo_relatorio == 'tipo_tarefa_agrupados':
        categorias.extend(sorted(list(MAPEAMENTO_PLANO_TAREFAS_DETALHADO.keys())))
        if 'Outros' not in categorias:
            categorias.append('Outros')
        if 'Não Categorizado' not in categorias:
            categorias.append('Não Categorizado')
    elif tipo_relatorio == 'tecnico_agrupados':
        categorias.extend(sorted(list(MAPEAMENTO_TECNICOS.keys())))
        if 'Outros' not in categorias:
            categorias.append('Outros')
        if 'Não Mapeado' not in categorias:
            categorias.append('Não Mapeado')

    return JsonResponse({'categorias': categorias})