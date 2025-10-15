from django.shortcuts import render
from django.db.models import Count, F, Q, Case, When, Value, CharField
from django.db.models.functions import TruncMonth, ExtractWeek
from .models import OrdemDeServico, Tarefa
from datetime import datetime, timedelta
from django.http import JsonResponse, HttpResponse
from django.shortcuts import redirect
from django.urls import reverse
import pandas as pd
import io
import re
from functools import reduce
import operator
from collections import defaultdict

# --- LÓGICA CENTRALIZADA DE AGRUPAMENTO POR LOCAL ---

# Lista de palavras-chave. A ordem não importa mais.
KEYWORDS_LOCAIS = [
    "ADAMA", "ADM BRASIL", "ALPHAVILLE", "ARCELORMITTAL", "ASSEMBLEIA", "BALL", "BIC", "CANOAS", "CPFL", "CSN",
    "ENGELOG", "FITESA", "FUNDAÇÃO BANRISUL", "GERDAU", "HOSPITAL DE CLINICAS", "LOJA MAÇONICA", "M DIAS", "NEOENERGIA",
    "PARK SHOPPING CANOAS", "PORTO DO AÇU", "RAIZEN", "SICOOB", "SIMEC", "SUMESA", "TRÊS CORAÇÕES", "TRT4", "TURIS",
    "UFRGS IPH", "UNILEVER", "UFRGS"
]


def get_grupo_local(local_str):
    """
    Função auxiliar que recebe uma string de local e retorna o grupo correto
    baseado na regra de "prioridade da esquerda".
    """
    if not local_str:
        return 'Outros'

    local_upper = local_str.upper()
    melhor_match = None
    menor_indice = float('inf')

    for keyword in KEYWORDS_LOCAIS:
        indice = local_upper.find(keyword.upper())
        if indice != -1 and indice < menor_indice:
            menor_indice = indice
            melhor_match = keyword.title()

    return melhor_match if melhor_match else 'Outros'


# --- FIM DA LÓGICA CENTRALIZADA ---


def _calculate_sla_for_group(ativo_code, sla_hours):
    """
    Função auxiliar que calcula o SLA para um grupo específico (C0, C1, ou C2).
    """
    base_query = OrdemDeServico.objects.filter(
        Local_Empresa__icontains='gerdau', tarefas__Ativo__icontains=ativo_code,
        Data_Criacao_OS__isnull=False, Data_Iniciou_OS__isnull=False
    ).distinct()
    query_with_duration = base_query.annotate(tempo_resposta=F('Data_Iniciou_OS') - F('Data_Criacao_OS'))
    atendido = query_with_duration.filter(tempo_resposta__lte=timedelta(hours=sla_hours)).count()
    nao_atendido = query_with_duration.filter(tempo_resposta__gt=timedelta(hours=sla_hours)).count()
    return {'atendido': atendido, 'nao_atendido': nao_atendido}


def dashboard_view(request):
    start_date_str = request.GET.get('start_date')
    end_date_str = request.GET.get('end_date')

    ordens_no_periodo = OrdemDeServico.objects.all()

    if start_date_str and end_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
            ordens_no_periodo = ordens_no_periodo.filter(Data_Criacao_OS__gte=start_date, Data_Criacao_OS__lte=end_date)
        except (ValueError, TypeError):
            pass
    else:
        start_date_str, end_date_str = '', ''

    # --- KPIs ---
    os_abertas_no_periodo = ordens_no_periodo.count()
    os_concluidas = ordens_no_periodo.filter(Status='Concluído').count()
    os_canceladas = ordens_no_periodo.filter(Status='Cancelado').count()
    os_em_processo = ordens_no_periodo.filter(Status='Em Processo').count()
    os_em_verificacao = ordens_no_periodo.filter(Status='Em Verificação').count()

    # --- Consultas para Gráficos ---
    os_por_status = ordens_no_periodo.values('Status').annotate(total=Count('Status')).order_by('-total')
    os_por_criticidade = ordens_no_periodo.values('Nivel_de_Criticidade').annotate(
        total=Count('Nivel_de_Criticidade')).order_by('-total')
    os_por_ticket = ordens_no_periodo.exclude(Possui_Ticket__isnull=True).values('Possui_Ticket').annotate(
        total=Count('id')).order_by('Possui_Ticket')
    os_por_mes = (OrdemDeServico.objects.annotate(mes=TruncMonth('Data_Criacao_OS')).values('mes').annotate(
        total=Count('id')).order_by('mes'))

    os_ativas = ordens_no_periodo.exclude(Status__in=['Concluído', 'Cancelado'])
    em_andamento_ids = set(Tarefa.objects.filter(
        ordem_de_servico__in=os_ativas, Status_da_Tarefa='IN_PROGRESS'
    ).values_list('ordem_de_servico_id', flat=True))
    pausadas_ids = set(Tarefa.objects.filter(
        ordem_de_servico__in=os_ativas, Status_da_Tarefa='PAUSED'
    ).exclude(ordem_de_servico_id__in=em_andamento_ids).values_list('ordem_de_servico_id', flat=True))
    ids_ja_classificadas = em_andamento_ids.union(pausadas_ids)
    nao_iniciadas_ids = set(os_ativas.exclude(id__in=ids_ja_classificadas).values_list('id', flat=True))
    em_andamento_count = len(em_andamento_ids)
    pausadas_count = len(pausadas_ids)
    nao_iniciadas_count = len(nao_iniciadas_ids)

    # --- Lógica para Gráfico de OS por Local/Cliente com Prioridade da Esquerda ---
    contagem_por_local = (ordens_no_periodo
                          .exclude(Local_Empresa__isnull=True).exclude(Local_Empresa__exact='')
                          .values('Local_Empresa')
                          .annotate(total=Count('id'))
                          )
    grupos_finais = defaultdict(int)
    for item in contagem_por_local:
        grupo = get_grupo_local(item['Local_Empresa'])
        grupos_finais[grupo] += item['total']

    grupos_ordenados = sorted(grupos_finais.items(), key=lambda x: x[1], reverse=True)
    local_agrupado_labels = [item[0] for item in grupos_ordenados]
    local_agrupado_data = [item[1] for item in grupos_ordenados]

    sla_c0 = _calculate_sla_for_group('(C0)', 3)
    sla_c1 = _calculate_sla_for_group('(C1)', 12)
    sla_c2 = _calculate_sla_for_group('(C2)', 24)

    anos_disponiveis = OrdemDeServico.objects.values_list('Ano_Criacao', flat=True).distinct().order_by('-Ano_Criacao')
    selected_year_weekly = request.GET.get('selected_year_weekly')
    weekly_labels, weekly_data_points = [], []
    if selected_year_weekly:
        try:
            selected_year_weekly = int(selected_year_weekly)
            weekly_counts_from_db = (OrdemDeServico.objects.filter(Ano_Criacao=selected_year_weekly).annotate(
                semana=ExtractWeek('Data_Criacao_OS')).values('semana').annotate(total=Count('id')).order_by('semana'))
            counts_dict = {item['semana']: item['total'] for item in weekly_counts_from_db}
            weekly_labels = [f"Sem {i}" for i in range(1, 54)]
            weekly_data_points = [counts_dict.get(i, 0) for i in range(1, 54)]
        except (ValueError, TypeError):
            selected_year_weekly = None

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
        'local_agrupado_labels': local_agrupado_labels,
        'local_agrupado_data': local_agrupado_data,
        'start_date': start_date_str, 'end_date': end_date_str,
        'anos_disponiveis': anos_disponiveis, 'selected_year_weekly': selected_year_weekly,
        'weekly_labels': weekly_labels, 'weekly_data_points': weekly_data_points,
        'sla_c0_data': sla_c0, 'sla_c1_data': sla_c1, 'sla_c2_data': sla_c2,
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
        OrdemDeServico.objects.filter(Local_Empresa__icontains='gerdau', tarefas__Ativo__icontains=sla_info['code'],
                                      Data_Criacao_OS__isnull=False, Data_Iniciou_OS__isnull=False).distinct().annotate(
            tempo_resposta=F('Data_Iniciou_OS') - F('Data_Criacao_OS')).filter(
            tempo_resposta__gt=timedelta(hours=sla_info['hours'])).order_by('-Data_Criacao_OS'))
    context = {'os_violadas': os_violadas, 'grupo_sla': grupo_sla, 'sla_hours': sla_info['hours'], }
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
        elif tipo_relatorio == 'criticidade':
            base_queryset = base_queryset.filter(Nivel_de_Criticidade=categoria)
        elif tipo_relatorio == 'ticket':
            base_queryset = base_queryset.filter(Possui_Ticket=categoria)
        elif tipo_relatorio == 'execucao':
            os_ativas = base_queryset.exclude(Status__in=['Concluído', 'Cancelado'])
            em_andamento_ids = set(
                Tarefa.objects.filter(ordem_de_servico__in=os_ativas, Status_da_Tarefa='IN_PROGRESS').values_list(
                    'ordem_de_servico_id', flat=True))
            pausadas_ids = set(Tarefa.objects.filter(ordem_de_servico__in=os_ativas, Status_da_Tarefa='PAUSED').exclude(
                ordem_de_servico_id__in=em_andamento_ids).values_list('ordem_de_servico_id', flat=True))
            ids_ja_classificadas = em_andamento_ids.union(pausadas_ids)
            nao_iniciadas_ids = set(os_ativas.exclude(id__in=ids_ja_classificadas).values_list('id', flat=True))

            if categoria == 'Em Andamento':
                base_queryset = base_queryset.filter(id__in=em_andamento_ids)
            elif categoria == 'Pausadas':
                base_queryset = base_queryset.filter(id__in=pausadas_ids)
            elif categoria == 'Não Iniciadas':
                base_queryset = base_queryset.filter(id__in=nao_iniciadas_ids)

        elif tipo_relatorio == 'locais_agrupados':
            ids_para_filtrar = []
            os_com_locais = base_queryset.exclude(Local_Empresa__isnull=True).values('id', 'Local_Empresa')
            for os in os_com_locais:
                grupo = get_grupo_local(os['Local_Empresa'])
                if grupo == categoria:
                    ids_para_filtrar.append(os['id'])
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

    context = {
        'ordens_de_servico': dados_filtrados,
        'total': dados_filtrados.count(),
        'tipo_relatorio': tipo_relatorio,
        'tipo_relatorio_display': tipo_relatorio.replace("_", " ").title() if tipo_relatorio else '',
        'categoria_display': categoria if categoria != 'todas' else 'Todas as Categorias',
        'start_date': start_date,
        'end_date': end_date,
    }
    return render(request, 'Relatorio/resultado_extracao.html', context)


def gerar_excel_view(request):
    tipo_relatorio = request.GET.get('tipo_relatorio')
    categoria = request.GET.get('categoria')
    start_date = request.GET.get('start_date')
    end_date = request.GET.get('end_date')

    dados_filtrados = _get_dados_filtrados(tipo_relatorio, categoria, start_date, end_date)

    dados_para_excel = dados_filtrados.values(
        'OS', 'Status', 'Nivel_de_Criticidade', 'Local_Empresa', 'Criado_Por',
        'Data_Criacao_OS', 'Data_Iniciou_OS', 'Data_Finalizacao_OS',
        'Avanco_da_OS', 'Possui_Ticket', 'Ticket_ID', 'Observacao_OS'
    )
    df = pd.DataFrame(list(dados_para_excel))

    colunas_de_data = ['Data_Criacao_OS', 'Data_Iniciou_OS', 'Data_Finalizacao_OS']
    for coluna in colunas_de_data:
        if coluna in df.columns:
            df[coluna] = df[coluna].apply(lambda x: x.tz_localize(None) if pd.notnull(x) else x)

    df.rename(columns={
        'OS': 'OS', 'Status': 'Status', 'Nivel_de_Criticidade': 'Criticidade',
        'Local_Empresa': 'Local/Empresa', 'Criado_Por': 'Criado Por',
        'Data_Criacao_OS': 'Data Criação', 'Data_Iniciou_OS': 'Data Início',
        'Data_Finalizacao_OS': 'Data Finalização', 'Avanco_da_OS': 'Avanço (%)',
        'Possui_Ticket': 'Possui Ticket?', 'Ticket_ID': 'ID do Ticket',
        'Observacao_OS': 'Observação'
    }, inplace=True)

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
        qs = OrdemDeServico.objects.exclude(Status__isnull=True).exclude(Status='').values_list('Status',
                                                                                                flat=True).distinct()
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