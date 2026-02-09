from django.shortcuts import render
from django.db.models import Count, F, Q, Value
from django.db.models.functions import TruncMonth, ExtractWeek, Now, Coalesce
from django.db.models import Avg, ExpressionWrapper, DurationField
from dateutil.relativedelta import relativedelta
from django.contrib.auth.views import LoginView, PasswordChangeView, LogoutView
from django.contrib.auth.decorators import login_required, user_passes_test
from django.urls import reverse_lazy
from django.contrib import messages
from django.utils.timezone import localtime
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
from django.shortcuts import render
from django.db.models import Count, Q
from django.db.models.functions import TruncDate
import json

from .models_gitel import (
    Feacsg, Feadiv, Feagsp, L1Pinda, Lam2Csg, 
    Lw01Pinda, Puccsg, Shrcsg, Shrgsp
)

def eh_o_usuario_permitido(user):
    return user.username == 'WesleyADM'

@login_required
@user_passes_test(eh_o_usuario_permitido)
def dashboard_eventos_gitel(request):
    mapa_tabelas = {
        'feacsg': {'model': Feacsg, 'nome': 'CSG ACI'},
        'shrcsg': {'model': Shrcsg, 'nome': 'CSG SHR'},
        'puccsg': {'model': Puccsg, 'nome': 'CSG PSUC'},
        'lam2csg': {'model': Lam2Csg, 'nome': 'CSG LAM2'},
        'feadiv': {'model': Feadiv, 'nome': 'DIV'},
        'feagsp': {'model': Feagsp, 'nome': 'GSP ACI'},
        'shrgsp': {'model': Shrgsp, 'nome': 'GSP SHR'},
        'l1pinda': {'model': L1Pinda, 'nome': 'PINDA LAM'},
    }

    tabela_selecionada = request.GET.get('tabela', 'feacsg')
    if tabela_selecionada not in mapa_tabelas:
        tabela_selecionada = 'feacsg'

    ModelClass = mapa_tabelas[tabela_selecionada]['model']
    nome_exibicao = mapa_tabelas[tabela_selecionada]['nome']

    # FILTROS DE DATA
    today = datetime.now().date()
    selected_periodo = request.GET.get('periodo', 'este_mes')
    start_date_str = request.GET.get('start_date', '')
    end_date_str = request.GET.get('end_date', '')

    if selected_periodo == 'este_mes':
        start_date = today.replace(day=1)
        end_date = today
    elif selected_periodo == '1_semana':
        start_date = today - timedelta(days=6)
        end_date = today
    elif selected_periodo == '3_meses':
        start_date = today - relativedelta(months=3)
        end_date = today
    elif selected_periodo == '6_meses':
        start_date = today - relativedelta(months=6)
        end_date = today
    elif selected_periodo == 'custom':
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            start_date = today.replace(day=1)
            end_date = today
    else:
        selected_periodo = 'este_mes'
        start_date = today.replace(day=1)
        end_date = today

    end_date_query = end_date + timedelta(days=1)

    # QUERY PRINCIPAL
    qs = ModelClass.objects.using('gitel_gerdau').filter(
        color__in=['Green', 'Red'],
        startdate__gte=start_date,
        startdate__lt=end_date_query
    )

    # KPIs
    count_green = qs.filter(color='Green').count()
    count_red = qs.filter(color='Red').count()
    
    total_green_real = max(0, count_green - count_red)
    total_red_exibido = count_red
    total_geral = qs.count()

    # GRÁFICO 1: STATUS POR CÂMERA
    por_camera = qs.values('cameras').annotate(
        ok=Count('pk', filter=Q(color='Green')),
        nok=Count('pk', filter=Q(color='Red')),
        total=Count('pk')
    ).order_by('-total')

    cameras_labels = []
    data_camera_green = []
    data_camera_red = []

    for x in por_camera:
        cam = x['cameras']
        g = x['ok']
        r = x['nok']
        real_g = max(0, g - r)
        
        cameras_labels.append(cam)
        data_camera_green.append(real_g)
        data_camera_red.append(r)

    # GRÁFICO 2 EVOLUÇÃO DIÁRIA
    por_dia = qs.annotate(data=TruncDate('startdate')).values('data').annotate(
        ok=Count('pk', filter=Q(color='Green')),
        nok=Count('pk', filter=Q(color='Red'))
    ).order_by('data')

    datas_labels = [x['data'].strftime('%d/%m') for x in por_dia if x['data']]
    data_dia_green = []
    data_dia_red = []

    for x in por_dia:
        g = x['ok']
        r = x['nok']
        real_g = max(0, g - r)
        data_dia_green.append(real_g)
        data_dia_red.append(r)

    dados_risco = defaultdict(lambda: defaultdict(lambda: {'Green': 0, 'Red': 0}))
    total_por_camera = defaultdict(int) 
    siglas_validas = ['AR', 'HC', 'FF', 'HM', 'NP', 'EPI']

    for item in qs:
        if item.title and item.cameras:
            nome_cam = item.cameras.strip()
            try:
                partes = item.title.strip().split('_')
                if len(partes) > 0:
                    sufixo = partes[-1].upper()
                    if sufixo not in siglas_validas and len(partes) > 1:
                         sufixo_alternativo = partes[-2].upper()
                         if sufixo_alternativo in siglas_validas:
                             sufixo = sufixo_alternativo

                    if sufixo in siglas_validas:
                        if item.color == 'Green':
                            dados_risco[nome_cam][sufixo]['Green'] += 1
                        elif item.color == 'Red':
                            dados_risco[nome_cam][sufixo]['Red'] += 1
            except Exception:
                continue

    cameras_validas = [] 

    for cam, siglas in dados_risco.items():
        soma_real_camera = 0
        for sigla in siglas_validas:
            g = siglas[sigla]['Green']
            r = siglas[sigla]['Red']
            valor_real = max(0, g - r)
            soma_real_camera += valor_real
        
        total_por_camera[cam] = soma_real_camera

        if soma_real_camera > 0:
            cameras_validas.append(cam)

    cameras_ordenadas_risco = sorted(cameras_validas, key=lambda k: total_por_camera[k], reverse=True)

    risco_labels = cameras_ordenadas_risco
    data_ar = []
    data_hc = []
    data_ff = []
    data_hm = []
    data_np = []
    data_epi = []

    for cam in cameras_ordenadas_risco:
        data_ar.append(max(0, dados_risco[cam]['AR']['Green'] - dados_risco[cam]['AR']['Red']))
        data_hc.append(max(0, dados_risco[cam]['HC']['Green'] - dados_risco[cam]['HC']['Red']))
        data_ff.append(max(0, dados_risco[cam]['FF']['Green'] - dados_risco[cam]['FF']['Red']))
        data_hm.append(max(0, dados_risco[cam]['HM']['Green'] - dados_risco[cam]['HM']['Red']))
        data_np.append(max(0, dados_risco[cam]['NP']['Green'] - dados_risco[cam]['NP']['Red']))
        data_epi.append(max(0, dados_risco[cam]['EPI']['Green'] - dados_risco[cam]['EPI']['Red']))

    context = {
        'mapa_tabelas': mapa_tabelas,
        'tabela_atual': tabela_selecionada,
        'nome_exibicao': nome_exibicao,
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
        'selected_periodo': selected_periodo,
        'total_geral': total_geral,
        'total_green': total_green_real,
        'total_red': total_red_exibido,
        'cameras_labels': cameras_labels,
        'data_camera_green': data_camera_green,
        'data_camera_red': data_camera_red,
        'datas_labels': datas_labels,
        'data_dia_green': data_dia_green,
        'data_dia_red': data_dia_red,
        'risco_labels': risco_labels,
        'data_ar': data_ar,
        'data_hc': data_hc,
        'data_ff': data_ff,
        'data_hm': data_hm,
        'data_np': data_np,
        'data_epi': data_epi, 
    }

    return render(request, 'Relatorio/dashboard_eventos.html', context)

class CustomLoginView(LoginView):
    template_name = 'Relatorio/login.html'
    redirect_authenticated_user = True

class CustomLogoutView(LogoutView):
    next_page = reverse_lazy('login')

class ForcePasswordChangeView(PasswordChangeView):
    template_name = 'Relatorio/force_password_change.html'
    success_url = reverse_lazy('dashboard') 

    def form_valid(self, form):
        response = super().form_valid(form)
        
        profile = self.request.user.profile
        profile.force_password_change = False
        profile.save()
        
        messages.success(self.request, "Senha atualizada com sucesso!")
        return response

# Normalização de texto para mapeamento
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

# Categorização Técnico
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

# Categorização Tipo de Tarefa
def get_grupo_tipo_tarefa(tipo_tarefa_str):
    if not tipo_tarefa_str:
        return 'Não Categorizado'
    tipo_normalizado = ' '.join(tipo_tarefa_str.strip().split()).lower()
    return TIPO_TAREFA_PARA_GRUPO.get(tipo_normalizado, 'Outros')

# Categorização Local
def get_grupo_local(local_str):
    if not local_str: return 'Outros'
    local_upper = local_str.upper()

    if 'GERDAU' in local_upper:
        return 'Gerdau'

    melhor_match, menor_indice = None, float('inf')
    for keyword in KEYWORDS_LOCAIS:
        indice = local_upper.find(keyword.upper())
        if indice != -1 and indice < menor_indice:
            menor_indice, melhor_match = indice, keyword.title()
    return melhor_match if melhor_match else 'Outros'

# View da Página Overview
@login_required
def Overview_view(request):
    clientes_target_keywords_upper = [
        "GERDAU", "LOJA MAÇONICA", "HOSPITAL DE CLINICAS", "PARK SHOPPING CANOAS",
        "TRT", "UNILEVER", "FUNDAÇÃO BANRISUL", "CSN", "CPFL", "ALPHAVILLE", "ASSEMBLEIA"
    ]

    clientes_target_formatados = [kw.title() for kw in clientes_target_keywords_upper]

    # Tratamento das Datas 
    today = datetime.now().date()
    selected_periodo = request.GET.get('periodo', '6_meses')
    
    start_date_str = request.GET.get('start_date', '')
    end_date_str = request.GET.get('end_date', '')

    expand = request.GET.get('expand')

    if selected_periodo == 'este_mes':
        start_date = today.replace(day=1)
        end_date = today
    elif selected_periodo == '1_semana':
        start_date = today - timedelta(days=6)
        end_date = today
    elif selected_periodo == '3_meses':
        start_date = today - relativedelta(months=3)
        end_date = today
    elif selected_periodo == '6_meses':
        start_date = today - relativedelta(months=6)
        end_date = today
    elif selected_periodo == 'custom':
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            start_date = (today - relativedelta(months=6)).replace(day=1)
            end_date = today
    else:
        selected_periodo = '6_meses'
        start_date = today - relativedelta(months=6)
        end_date = today

    end_date_query = end_date + timedelta(days=1)

    if not start_date_str:
        start_date_str = start_date.strftime('%Y-%m-%d')
    if not end_date_str:
        end_date_str = end_date.strftime('%Y-%m-%d')

    os_no_periodo_filtrado = OrdemDeServico.objects.filter(
        Data_Criacao_OS__gte=start_date,
        Data_Criacao_OS__lt=end_date_query
    )

    # Gráfico OS por Mês
    os_por_mes = (
        OrdemDeServico.objects
        .annotate(mes=TruncMonth('Data_Criacao_OS'))
        .values('mes')
        .annotate(total=Count('id'))
        .order_by('mes')
    )
    mes_labels = [d['mes'].strftime('%b/%Y') for d in os_por_mes if d['mes']]
    mes_data = [d['total'] for d in os_por_mes if d['mes']]

    # Cálculo dos Dados por Cliente
    dados_agrupados_por_cliente = defaultdict(lambda: {
        'total_os': 0, 'em_processo': 0, 'em_verificacao': 0,
        'concluidas': 0, 'canceladas': 0,
        'soma_tempo_atendimento': timedelta(0), 'contagem_tempo_atendimento': 0,
        'sla_status': 'N/A'
    })

    expanded_cliente = None
    if expand and expand.lower() == 'gerdau':
        expanded_cliente = 'Gerdau'
        grupos = {}

        def novo_resumo(nome):
            return {
                'cliente': nome,
                'total_os': 0, 'em_processo': 0, 'em_verificacao': 0,
                'concluidas': 0, 'canceladas': 0,
                'soma_tempo_atendimento': timedelta(0), 'contagem_tempo_atendimento': 0,
                'sla_cumprido': "N/A", 'sla_violado': "N/A"
            }

        gerdau_qs = os_no_periodo_filtrado.filter(
            Local_Empresa__icontains='gerdau'
        ).annotate(
            tempo_atendimento_calculado=ExpressionWrapper(
                Coalesce(F('Data_Enviado_Verificacao'), Value(None)) - Coalesce(F('Data_Criacao_OS'), Value(None)),
                output_field=DurationField()
            )
        ).values('Local_Empresa', 'Status', 'tempo_atendimento_calculado')

        for osd in gerdau_qs:
            local = (osd.get('Local_Empresa') or '').upper()
            # tenta encontrar keyword específica que apareça no local
            subgroup = None
            for kw in KEYWORDS_LOCAIS:
                if kw.upper().startswith('GERDAU') and kw.upper() in local:
                    subgroup = kw.title()
                    break
            if not subgroup:
                subgroup = 'Gerdau Outros'

            resumo = grupos.setdefault(subgroup, novo_resumo(subgroup))
            resumo['total_os'] += 1
            st = osd.get('Status')
            if st == 'Concluído':
                resumo['concluidas'] += 1
            elif st == 'Em Processo':
                resumo['em_processo'] += 1
            elif st == 'Em Verificação':
                resumo['em_verificacao'] += 1
            elif st == 'Cancelado':
                resumo['canceladas'] += 1

            if osd.get('tempo_atendimento_calculado') is not None:
                resumo['soma_tempo_atendimento'] += osd['tempo_atendimento_calculado']
                resumo['contagem_tempo_atendimento'] += 1

        # monta data_resumo a partir dos grupos encontrados
        data_resumo = []
        for nome, dados in grupos.items():
            tempo_medio = None
            if dados['contagem_tempo_atendimento'] > 0:
                tempo_medio = dados['soma_tempo_atendimento'] / dados['contagem_tempo_atendimento']
            data_resumo.append({
                'cliente': dados['cliente'],
                'total_os': dados['total_os'],
                'em_processo': dados['em_processo'],
                'em_verificacao': dados['em_verificacao'],
                'concluidas': dados['concluidas'],
                'canceladas': dados['canceladas'],
                'tempo_medio_atendimento': tempo_medio,
                'sla_cumprido': dados.get('sla_cumprido', "N/A"),
                'sla_violado': dados.get('sla_violado', "N/A")
            })
        data_resumo.sort(key=lambda item: item['cliente'])
    else:
        expanded_cliente = None

    # Exceção Hospital de Clínicas Busca Por Ativo e Local
    ids_hc_ativo = set(Tarefa.objects.filter(
        ordem_de_servico__in=os_no_periodo_filtrado,
        Ativo__icontains='HOSPITAL DE CLINICAS'
    ).values_list('ordem_de_servico_id', flat=True))

    ids_hc_local = set(os_no_periodo_filtrado.filter(
        Local_Empresa__icontains='HOSPITAL DE CLINICAS'
    ).values_list('id', flat=True))

    all_hc_ids = ids_hc_ativo.union(ids_hc_local)  

    os_no_periodo_outros = os_no_periodo_filtrado.exclude(id__in=all_hc_ids) \
        .annotate(
        tempo_atendimento_calculado=ExpressionWrapper(
            Coalesce(F('Data_Enviado_Verificacao'), Value(None)) - Coalesce(F('Data_Criacao_OS'), Value(None)),
            output_field=DurationField()
        )
    ).values('Local_Empresa', 'Status', 'tempo_atendimento_calculado')

    # Busca por Local_Empresa
    for os_data in os_no_periodo_outros:
        cliente_grupo = get_grupo_local(os_data['Local_Empresa'])

        if cliente_grupo in clientes_target_formatados:
            grupo_data = dados_agrupados_por_cliente[cliente_grupo]
            grupo_data['total_os'] += 1
            if os_data['Status'] == 'Concluído':
                grupo_data['concluidas'] += 1
            elif os_data['Status'] == 'Em Processo':
                grupo_data['em_processo'] += 1
            elif os_data['Status'] == 'Em Verificação':
                grupo_data['em_verificacao'] += 1
            elif os_data['Status'] == 'Cancelado':
                grupo_data['canceladas'] += 1

            if os_data['tempo_atendimento_calculado'] is not None:
                grupo_data['soma_tempo_atendimento'] += os_data['tempo_atendimento_calculado']
                grupo_data['contagem_tempo_atendimento'] += 1

    # 5. Obter dados para as OSs que SÃO HC 
    if all_hc_ids and 'Hospital De Clinicas' in clientes_target_formatados:
        os_no_periodo_hc = OrdemDeServico.objects.filter(
            id__in=all_hc_ids
        ).annotate(
            tempo_atendimento_calculado=ExpressionWrapper(
                Coalesce(F('Data_Enviado_Verificacao'), Value(None)) - Coalesce(F('Data_Criacao_OS'), Value(None)),
                output_field=DurationField()
            )
        ).values('Status', 'tempo_atendimento_calculado')

        # 6. Loop para processar o cliente HC
        grupo_data_hc = dados_agrupados_por_cliente['Hospital De Clinicas']
        for os_data in os_no_periodo_hc:
            grupo_data_hc['total_os'] += 1
            if os_data['Status'] == 'Concluído':
                grupo_data_hc['concluidas'] += 1
            elif os_data['Status'] == 'Em Processo':
                grupo_data_hc['em_processo'] += 1
            elif os_data['Status'] == 'Em Verificação':
                grupo_data_hc['em_verificacao'] += 1
            elif os_data['Status'] == 'Cancelado':
                grupo_data_hc['canceladas'] += 1

            if os_data['tempo_atendimento_calculado'] is not None:
                grupo_data_hc['soma_tempo_atendimento'] += os_data['tempo_atendimento_calculado']
                grupo_data_hc['contagem_tempo_atendimento'] += 1
    
    #SLAs
    sla_data_gerdau_c0 = _calculate_resolution_sla_for_group(os_no_periodo_filtrado, '(C0)', 8)
    sla_data_gerdau_c1 = _calculate_resolution_sla_for_group(os_no_periodo_filtrado, '(C1)', 24)
    sla_data_gerdau_c2 = _calculate_resolution_sla_for_group(os_no_periodo_filtrado, '(C2)', 48)
    
    sla_total_gerdau = {
        'atendido': sla_data_gerdau_c0['atendido'] + sla_data_gerdau_c1['atendido'] + sla_data_gerdau_c2['atendido'],
        'nao_atendido': sla_data_gerdau_c0['nao_atendido'] + sla_data_gerdau_c1['nao_atendido'] + sla_data_gerdau_c2['nao_atendido']
    }
    
    sla_data_parkshopping = _calculate_parkshopping_sla(os_no_periodo_filtrado, 24)
    sla_data_cpfl = _calculate_cpfl_sla(os_no_periodo_filtrado, 72)
    
    sla_map_final = {
        'Gerdau': sla_total_gerdau,
        'Park Shopping Canoas': sla_data_parkshopping,
        'Cpfl': sla_data_cpfl
    }

    if not expanded_cliente:
        data_resumo = []
        for cliente, dados in dados_agrupados_por_cliente.items():
            tempo_medio = None
            if dados['contagem_tempo_atendimento'] > 0:
                tempo_medio = dados['soma_tempo_atendimento'] / dados['contagem_tempo_atendimento']

            sla_info = sla_map_final.get(cliente)
            sla_cumprido = "N/A"
            sla_violado = "N/A"
            if sla_info and (sla_info['atendido'] > 0 or sla_info['nao_atendido'] > 0):
                sla_cumprido = sla_info['atendido']
                sla_violado = sla_info['nao_atendido']

            data_resumo.append({
                'cliente': cliente,
                'total_os': dados['total_os'],
                'em_processo': dados['em_processo'],
                'em_verificacao': dados['em_verificacao'],
                'concluidas': dados['concluidas'],
                'canceladas': dados['canceladas'],
                'tempo_medio_atendimento': tempo_medio,
                'sla_cumprido': sla_cumprido,
                'sla_violado': sla_violado
            })
        data_resumo.sort(key=lambda item: item['cliente'])

    # Gráfico OS por Ticket 
    os_por_ticket_periodo = os_no_periodo_filtrado.exclude(
        Possui_Ticket__isnull=True
    ).values('Possui_Ticket').annotate(total=Count('id')).order_by('Possui_Ticket')
    ticket_labels_periodo = []
    for item in os_por_ticket_periodo:
        label = item['Possui_Ticket']
        if label == 'Sim': ticket_labels_periodo.append('Com Ticket')
        elif label == 'Não': ticket_labels_periodo.append('Sem Ticket')
        else: ticket_labels_periodo.append(label)
    ticket_data_periodo = [item['total'] for item in os_por_ticket_periodo]

    # Gráfico Tipo de Tarefa
    tarefas_tipos_no_periodo_filtrado = Tarefa.objects.filter(
        ordem_de_servico__in=os_no_periodo_filtrado
    ).exclude(Tipo_de_Tarefa__isnull=True).exclude(Tipo_de_Tarefa__exact=''
                                                   ).values('ordem_de_servico_id', 'Tipo_de_Tarefa')
    grupos_tipo_por_os_periodo = defaultdict(set)
    for tarefa_data in tarefas_tipos_no_periodo_filtrado:
        os_id = tarefa_data['ordem_de_servico_id']
        tipo_tarefa = tarefa_data['Tipo_de_Tarefa']
        grupo_tipo = get_grupo_tipo_tarefa(tipo_tarefa)
        grupos_tipo_por_os_periodo[os_id].add(grupo_tipo)
    contagem_grupo_tipo_tarefa_periodo = defaultdict(int)
    for os_id, grupos_da_os in grupos_tipo_por_os_periodo.items():
        for grupo in grupos_da_os:
            contagem_grupo_tipo_tarefa_periodo[grupo] += 1
    tipo_tarefa_grupo_labels_periodo = list(contagem_grupo_tipo_tarefa_periodo.keys())
    tipo_tarefa_grupo_data_periodo = list(contagem_grupo_tipo_tarefa_periodo.values())

    # Gráfico Desempenho por Técnico
    statuses_interesse_tecnico = ['Em Processo', 'Em Verificação', 'Concluído']
    contagem_tecnico_status_resumo = defaultdict(lambda: defaultdict(int))
    os_ids_filtrados = os_no_periodo_filtrado.values_list('id', flat=True)
    tarefas_tecnicos_resumo = Tarefa.objects.filter(
        ordem_de_servico_id__in=os_ids_filtrados,
        ordem_de_servico__Status__in=statuses_interesse_tecnico
    ).exclude(Responsavel__isnull=True).exclude(Responsavel__exact=''
                                                ).select_related('ordem_de_servico').values(
        'ordem_de_servico_id', 'ordem_de_servico__Status', 'Responsavel'
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
        contagem_tecnico_status_resumo.items(), key=lambda item: sum(item[1].values()), reverse=True
    )
    tecnico_status_labels_resumo = [item[0] for item in tecnicos_ordenados_status_resumo]
    tecnico_status_data_processo_resumo = [item[1].get('Em Processo', 0) for item in tecnicos_ordenados_status_resumo]
    tecnico_status_data_verificacao_resumo = [item[1].get('Em Verificação', 0) for item in tecnicos_ordenados_status_resumo]
    tecnico_status_data_concluido_resumo = [item[1].get('Concluído', 0) for item in tecnicos_ordenados_status_resumo]

    try:
        gerdau_labels
    except NameError:
        gerdau_labels = []
    try:
        gerdau_data
    except NameError:
        gerdau_data = []

    context = {
        'expanded_cliente': expanded_cliente,
        'data_resumo': data_resumo,
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
        'selected_periodo': selected_periodo,
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
        'gerdau_labels': gerdau_labels,
        'gerdau_data': gerdau_data,
    }

    return render(request, 'Relatorio/Overview.html', context)


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

# SLA de Resolução - CPFL
def _calculate_cpfl_sla(base_queryset, sla_hours):
    query = base_queryset.filter(
        Local_Empresa__icontains='CPFL',
        Data_Criacao_OS__isnull=False, Data_Enviado_Verificacao__isnull=False
    ).exclude(tarefas__Tipo_de_Tarefa__icontains='Preventiva').distinct()
    query_with_duration = query.annotate(tempo_decorrido=F('Data_Enviado_Verificacao') - F('Data_Criacao_OS'))
    atendido = query_with_duration.filter(tempo_decorrido__lte=timedelta(hours=sla_hours)).count()
    nao_atendido = query_with_duration.filter(tempo_decorrido__gt=timedelta(hours=sla_hours)).count()
    return {'atendido': atendido, 'nao_atendido': nao_atendido}

@login_required
def dashboard_view(request):
    # 1. Tratamento de Datas
    start_date_str = request.GET.get('start_date')
    end_date_str = request.GET.get('end_date')
    selected_local = request.GET.get('local_grupo', 'Todos')
    
    start_date = None
    end_date = None
    
    if start_date_str and end_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        except ValueError:
            pass

    # 2. QuerySet Base Otimizado 
    qs = OrdemDeServico.objects.all().no_periodo(start_date, end_date)

    # 3. Filtro de Local
    if selected_local and selected_local != 'Todos':
        if selected_local == 'Gerdau Outros':
             qs = qs.filter(Local_Agrupado='Gerdau', Local_Detalhado='Gerdau Outros')
        else:
             qs = qs.filter(Local_Agrupado=selected_local)

    # 4. Métricas Gerais
    metrics = qs.aggregate(
        total=Count('id'),
        concluidas=Count('id', filter=Q(Status='Concluído')),
        canceladas=Count('id', filter=Q(Status='Cancelado')),
        em_processo=Count('id', filter=Q(Status='Em Processo')),
        em_verificacao=Count('id', filter=Q(Status='Em Verificação')),
        com_ticket=Count('id', filter=Q(Possui_Ticket='Sim')),
        sem_ticket=Count('id', filter=Q(Possui_Ticket='Não')),
    )
    # Percentuais
    total_os = metrics['total'] or 0
    
    def calc_pct(val):
        if total_os == 0: return "0,00"
        pct = (val / total_os) * 100
        return f"{pct:.2f}".replace('.', ',')

    pct_concluidas = calc_pct(metrics['concluidas'])
    pct_processo = calc_pct(metrics['em_processo'])
    pct_verificacao = calc_pct(metrics['em_verificacao'])
    pct_canceladas = calc_pct(metrics['canceladas'])

    # 5. Gráficos Principais 
    os_por_status = qs.values('Status').annotate(total=Count('id')).order_by('-total')
    
    os_por_criticidade = qs.values('Nivel_de_Criticidade').annotate(total=Count('id')).order_by('-total')

    os_por_local = qs.values('Local_Agrupado').annotate(total=Count('id')).order_by('-total')
    local_agrupado_labels = [item['Local_Agrupado'] for item in os_por_local if item['Local_Agrupado']]
    local_agrupado_data = [item['total'] for item in os_por_local if item['Local_Agrupado']]

    # Drilldown: Gerdau
    os_gerdau = qs.filter(Local_Agrupado='Gerdau').values('Local_Detalhado').annotate(total=Count('id')).order_by('-total')
    gerdau_labels = [item['Local_Detalhado'] for item in os_gerdau]
    gerdau_data = [item['total'] for item in os_gerdau]

    # Drilldown: Outros Locais
    os_outros_locais = qs.filter(Local_Agrupado='Outros').values('Local_Empresa').annotate(total=Count('id')).order_by('-total')
    outros_locais_labels = [item['Local_Empresa'] or 'Não Informado' for item in os_outros_locais]
    outros_locais_data = [item['total'] for item in os_outros_locais]

    # 6. Tarefas e Técnicos
    tarefas_qs = Tarefa.objects.filter(ordem_de_servico__in=qs)
    
    # Tarefas por Tipo
    por_tipo_tarefa = tarefas_qs.values('Tipo_Tarefa_Agrupado').annotate(
        total=Count('ordem_de_servico', distinct=True)
    ).order_by('-total')
    
    tipo_tarefa_grupo_labels = [t['Tipo_Tarefa_Agrupado'] for t in por_tipo_tarefa if t['Tipo_Tarefa_Agrupado']]
    tipo_tarefa_grupo_data = [t['total'] for t in por_tipo_tarefa if t['Tipo_Tarefa_Agrupado']]

    # Drilldown: Tipos de Tarefa Outros
    tipos_outros_qs = tarefas_qs.filter(Tipo_Tarefa_Agrupado='Outros').values('Tipo_de_Tarefa').annotate(
        total=Count('ordem_de_servico', distinct=True)
    ).order_by('-total')
    tipo_tarefa_outros_labels = [t['Tipo_de_Tarefa'] for t in tipos_outros_qs if t['Tipo_de_Tarefa']]
    tipo_tarefa_outros_data = [t['total'] for t in tipos_outros_qs if t['Tipo_de_Tarefa']]

    # Tarefas por Técnico
    por_tecnico = tarefas_qs.values('Responsavel_Agrupado').annotate(
        total=Count('ordem_de_servico', distinct=True)
    ).order_by('-total')
    
    tecnico_grupo_labels = [t['Responsavel_Agrupado'] for t in por_tecnico if t['Responsavel_Agrupado']]
    tecnico_grupo_data = [t['total'] for t in por_tecnico if t['Responsavel_Agrupado']]

    # Drilldown: Técnicos Outros
    tecnicos_outros_qs = tarefas_qs.filter(Responsavel_Agrupado='Outros').values('Responsavel').annotate(
        total=Count('ordem_de_servico', distinct=True)
    ).order_by('-total')
    
    tecnico_outros_labels = [t['Responsavel'] for t in tecnicos_outros_qs if t['Responsavel']]
    tecnico_outros_data = [t['total'] for t in tecnicos_outros_qs if t['Responsavel']]


    os_por_mes_qs = (
        qs.annotate(mes=TruncMonth('Data_Criacao_OS'))
        .values('mes')
        .annotate(total=Count('id'))
        .order_by('mes')
    )
    mes_labels = [d['mes'].strftime('%b/%Y') for d in os_por_mes_qs if d['mes']]
    mes_data = [d['total'] for d in os_por_mes_qs if d['mes']]

    # 8. Gráfico de Status de Execução
    os_ativas = qs.exclude(Status__in=['Concluído', 'Cancelado'])
    
    ids_com_tarefa_in_progress = set(
        Tarefa.objects.filter(ordem_de_servico__in=os_ativas, Status_da_Tarefa='IN_PROGRESS').values_list('ordem_de_servico_id', flat=True)
    )
    ids_com_avanco_e_processo = set(
        os_ativas.filter(Status='Em Processo', Avanco_da_OS__gt=0).values_list('id', flat=True)
    )
    em_andamento_ids = ids_com_tarefa_in_progress.union(ids_com_avanco_e_processo)
    
    candidatas_pausadas_ids = set(
        Tarefa.objects.filter(ordem_de_servico__in=os_ativas, Status_da_Tarefa='PAUSED').values_list('ordem_de_servico_id', flat=True)
    )
    ids_em_verificacao = set(os_ativas.filter(Status='Em Verificação').values_list('id', flat=True))
    
    pausadas_ids = candidatas_pausadas_ids - em_andamento_ids - ids_em_verificacao
    
    ids_ja_classificadas = em_andamento_ids.union(pausadas_ids)
    
    # Nao iniciadas
    nao_iniciadas_count = os_ativas.exclude(id__in=ids_ja_classificadas).filter(
        Q(Avanco_da_OS=0) | Q(Avanco_da_OS__isnull=True), Status='Em Processo'
    ).count()

    em_andamento_count = len(em_andamento_ids)
    pausadas_count = len(pausadas_ids)

    # 9. SLAs 
    sla_atendimento_c0 = _calculate_sla_for_group(qs, '(C0)', 3)
    sla_atendimento_c1 = _calculate_sla_for_group(qs, '(C1)', 12)
    sla_atendimento_c2 = _calculate_sla_for_group(qs, '(C2)', 24)

    sla_resolucao_c0 = _calculate_resolution_sla_for_group(qs, '(C0)', 8)
    sla_resolucao_c1 = _calculate_resolution_sla_for_group(qs, '(C1)', 24)
    sla_resolucao_c2 = _calculate_resolution_sla_for_group(qs, '(C2)', 48)

    sla_parkshopping_data = _calculate_parkshopping_sla(qs, 24)
    sla_cpfl_data = _calculate_cpfl_sla(qs, 72)

    # 10. Gráfico Semanal 
    anos_disponiveis = OrdemDeServico.objects.values_list('Ano_Criacao', flat=True).distinct().order_by('-Ano_Criacao')
    selected_year_weekly = request.GET.get('selected_year_weekly')
    weekly_labels, weekly_data_points = [], []
    
    if selected_year_weekly:
        try:
            selected_year_weekly = int(selected_year_weekly)
            weekly_counts = (
                OrdemDeServico.objects
                .filter(Ano_Criacao=selected_year_weekly)
                .annotate(semana=ExtractWeek('Data_Criacao_OS'))
                .values('semana')
                .annotate(total=Count('id'))
                .order_by('semana')
            )
            counts_dict = {item['semana']: item['total'] for item in weekly_counts if item['semana']}
            weekly_labels = [f"Sem {i}" for i in range(1, 54)]
            weekly_data_points = [counts_dict.get(i, 0) for i in range(1, 54)]
        except (ValueError, TypeError):
            selected_year_weekly = None

    context = {
        'os_abertas_no_periodo': metrics['total'],
        'os_concluidas': metrics['concluidas'],
        'os_em_processo': metrics['em_processo'],
        'os_em_verificacao': metrics['em_verificacao'],
        'os_canceladas': metrics['canceladas'],

        'pct_concluidas': pct_concluidas,
        'pct_processo': pct_processo,
        'pct_verificacao': pct_verificacao,
        'pct_canceladas': pct_canceladas,
        
        'status_labels': [item['Status'] for item in os_por_status],
        'status_data': [item['total'] for item in os_por_status],
        
        'criticidade_labels': [item['Nivel_de_Criticidade'] for item in os_por_criticidade],
        'criticidade_data': [item['total'] for item in os_por_criticidade],
        
        'ticket_labels': ['Com Ticket', 'Sem Ticket'],
        'ticket_data': [metrics['com_ticket'], metrics['sem_ticket']],
        
        'local_agrupado_labels': local_agrupado_labels,
        'local_agrupado_data': local_agrupado_data,
        
        'gerdau_labels': gerdau_labels,
        'gerdau_data': gerdau_data,
        'outros_locais_labels': outros_locais_labels,
        'outros_locais_data': outros_locais_data,
        
        'tecnico_grupo_labels': tecnico_grupo_labels,
        'tecnico_grupo_data': tecnico_grupo_data,
        'tecnico_outros_labels': tecnico_outros_labels,
        'tecnico_outros_data': tecnico_outros_data,
        
        'tipo_tarefa_grupo_labels': tipo_tarefa_grupo_labels,
        'tipo_tarefa_grupo_data': tipo_tarefa_grupo_data,
        'tipo_tarefa_outros_labels': tipo_tarefa_outros_labels,
        'tipo_tarefa_outros_data': tipo_tarefa_outros_data,

        'mes_labels': mes_labels,
        'mes_data': mes_data,
        
        'execucao_status_labels': ['Em Andamento', 'Pausadas', 'Não Iniciadas'],
        'execucao_status_data': [em_andamento_count, pausadas_count, nao_iniciadas_count],
        
        'sla_atendimento_c0_data': sla_atendimento_c0, 
        'sla_atendimento_c1_data': sla_atendimento_c1,
        'sla_atendimento_c2_data': sla_atendimento_c2,
        'sla_resolucao_c0_data': sla_resolucao_c0, 
        'sla_resolucao_c1_data': sla_resolucao_c1,
        'sla_resolucao_c2_data': sla_resolucao_c2,
        'sla_parkshopping_data': sla_parkshopping_data,
        'sla_cpfl_data': sla_cpfl_data,
        
        'anos_disponiveis': anos_disponiveis, 
        'selected_year_weekly': selected_year_weekly,
        'weekly_labels': weekly_labels, 
        'weekly_data_points': weekly_data_points,
        
        'start_date': start_date_str, 
        'end_date': end_date_str,
        'grupos_de_local_disponiveis': ['Todos'] + sorted(list(set(local_agrupado_labels))),
        'selected_local': selected_local,
    }

    return render(request, 'Relatorio/dashboard.html', context)


def sla_violado_view(request, grupo_sla):
    grupo_sla = grupo_sla.upper()
    sla_map = {
        'C0': {'hours': 3, 'code': '(C0)'}, 
        'C1': {'hours': 12, 'code': '(C1)'},
        'C2': {'hours': 24, 'code': '(C2)'}, 
    }
    
    if grupo_sla not in sla_map:
        return render(request, 'Relatorio/sla_violado.html', {'error': 'Grupo de SLA inválido.'})
    
    sla_info = sla_map[grupo_sla]
    
    start_date_str = request.GET.get('start_date')
    end_date_str = request.GET.get('end_date')

    # Query Base
    os_violadas = OrdemDeServico.objects.filter(
        Local_Empresa__icontains='gerdau', 
        tarefas__Ativo__icontains=sla_info['code'],
        Data_Criacao_OS__isnull=False, 
        Data_Iniciou_OS__isnull=False
    ).exclude(tarefas__Tipo_de_Tarefa__icontains='Preventiva')

    if start_date_str and end_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
            os_violadas = os_violadas.filter(Data_Criacao_OS__gte=start_date, Data_Criacao_OS__lte=end_date)
        except ValueError:
            pass

    os_violadas = (
        os_violadas.distinct()
        .annotate(tempo_decorrido=F('Data_Iniciou_OS') - F('Data_Criacao_OS'))
        .filter(tempo_decorrido__gt=timedelta(hours=sla_info['hours']))
        .order_by('-Data_Criacao_OS')
    )
    
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

    start_date_str = request.GET.get('start_date')
    end_date_str = request.GET.get('end_date')

    # Query Base
    os_violadas = OrdemDeServico.objects.filter(
        Local_Empresa__icontains='gerdau', 
        tarefas__Ativo__icontains=sla_info['code'],
        Data_Criacao_OS__isnull=False, 
        Data_Enviado_Verificacao__isnull=False
    ).exclude(tarefas__Tipo_de_Tarefa__icontains='Preventiva')

    if start_date_str and end_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
            os_violadas = os_violadas.filter(Data_Criacao_OS__gte=start_date, Data_Criacao_OS__lte=end_date)
        except ValueError:
            pass

    os_violadas = (
        os_violadas.distinct()
        .annotate(tempo_decorrido=F('Data_Enviado_Verificacao') - F('Data_Criacao_OS'))
        .filter(tempo_decorrido__gt=timedelta(hours=sla_info['hours']))
        .order_by('-Data_Criacao_OS')
    )
    
    context = {'os_violadas': os_violadas, 'grupo_sla': grupo_sla, 'sla_hours': sla_info['hours'], 'tipo_sla': 'Resolução'}
    return render(request, 'Relatorio/sla_violado.html', context)


def sla_parkshopping_violado_view(request):
    sla_hours = 24
    
    start_date_str = request.GET.get('start_date')
    end_date_str = request.GET.get('end_date')
    
    # QueryBase
    os_violadas = OrdemDeServico.objects.filter(
        Local_Empresa__icontains='PARK SHOPPING CANOAS',
        Data_Criacao_OS__isnull=False, 
        Data_Enviado_Verificacao__isnull=False
    ).exclude(tarefas__Tipo_de_Tarefa__icontains='Preventiva')

    if start_date_str and end_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
            
            os_violadas = os_violadas.filter(
                Data_Criacao_OS__gte=start_date,
                Data_Criacao_OS__lte=end_date
            )
        except ValueError:
            pass 

    os_violadas = os_violadas.distinct().annotate(
        tempo_decorrido=F('Data_Enviado_Verificacao') - F('Data_Criacao_OS')
    ).filter(
        tempo_decorrido__gt=timedelta(hours=sla_hours)
    ).order_by('-Data_Criacao_OS')

    context = {
        'os_violadas': os_violadas, 
        'sla_hours': sla_hours,
        'tipo_sla': 'Resolução'
    }
    return render(request, 'Relatorio/sla_violado.html', context)

def sla_cpfl_violado_view(request):
    sla_hours = 72
    
    start_date_str = request.GET.get('start_date')
    end_date_str = request.GET.get('end_date')

    # Query Base
    os_violadas = OrdemDeServico.objects.filter(
        Local_Empresa__icontains='CPFL',
        Data_Criacao_OS__isnull=False,
        Data_Enviado_Verificacao__isnull=False
    ).exclude(tarefas__Tipo_de_Tarefa__icontains='Preventiva')

    if start_date_str and end_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
            os_violadas = os_violadas.filter(Data_Criacao_OS__gte=start_date, Data_Criacao_OS__lte=end_date)
        except ValueError:
            pass

    os_violadas = (
        os_violadas.distinct()
        .annotate(tempo_decorrido=F('Data_Enviado_Verificacao') - F('Data_Criacao_OS'))
        .filter(tempo_decorrido__gt=timedelta(hours=sla_hours))
        .order_by('-Data_Criacao_OS')
    )

    context = {'os_violadas': os_violadas, 'sla_hours': sla_hours, 'tipo_sla': 'Resolução'}
    return render(request, 'Relatorio/sla_violado.html', context)


def _get_dados_filtrados(tipo_relatorio, categoria, start_date_str, end_date_str, filtro_local_extra=None):
    base_queryset = OrdemDeServico.objects.all()

    # 1. Filtro de Data
    if start_date_str and end_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
            base_queryset = base_queryset.filter(Data_Criacao_OS__gte=start_date, Data_Criacao_OS__lte=end_date)
        except (ValueError, TypeError):
            pass

    # Filtro de Local EXTRA (Só afeta o Modal do Dashboard)
    if filtro_local_extra and filtro_local_extra != 'Todos':
        if filtro_local_extra == 'Gerdau Outros':
             base_queryset = base_queryset.filter(Local_Agrupado='Gerdau', Local_Detalhado='Gerdau Outros')
        else:
             base_queryset = base_queryset.filter(Local_Agrupado=filtro_local_extra)

    # Anotações de Tempo 
    base_queryset = base_queryset.annotate(
        tempo_em_execucao=F('Data_Enviado_Verificacao') - F('Data_Criacao_OS'),
        tempo_em_verificacao=F('Data_Finalizacao_OS') - F('Data_Enviado_Verificacao')
    )

    # Filtros Específicos da Extração 
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
            todos_locais = sorted([kw.title() for kw in KEYWORDS_LOCAIS])
            gerdau_especificos = [l for l in todos_locais if l.upper().startswith('GERDAU')]
            
            base_queryset = base_queryset.filter(
                Q(Local_Empresa__icontains='gerdau') |
                Q(Local_Empresa__in=gerdau_especificos) |
                Q(Local_Empresa__isnull=True) |
                Q(Local_Empresa__isnull=False) 
            )
            
            cat_norm = (categoria or '').strip()
            
            if cat_norm.lower() == 'gerdau':
                base_queryset = base_queryset.filter(Local_Empresa__icontains='gerdau')
            
            elif cat_norm.lower() == 'gerdau outros':
                qs = base_queryset.filter(Local_Empresa__icontains='gerdau')
                for kw in KEYWORDS_LOCAIS:
                    if kw.upper().startswith('GERDAU'):
                        qs = qs.exclude(Local_Empresa__icontains=kw)
                base_queryset = qs
                
            elif any(cat_norm.lower() == kw.title().lower() for kw in KEYWORDS_LOCAIS):
                 base_queryset = base_queryset.filter(Local_Empresa__icontains=cat_norm)
            
            else:
                ids_para_filtrar = [
                    os['id']
                    for os in base_queryset.exclude(Local_Empresa__isnull=True).values('id', 'Local_Empresa')
                    if get_grupo_local(os['Local_Empresa']) == categoria
                ]
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

    dados_qs = _get_dados_filtrados(tipo_relatorio, categoria, start_date, end_date).prefetch_related('tarefas')

    def fmt_dt(dt):
        try:
            return localtime(dt).strftime('%d/%m/%Y %H:%M:%S') if dt else ''
        except Exception:
            return str(dt) if dt else ''

    linhas = []
    for os in dados_qs:
        # Mapeamento de Cliente
        cliente_grupo = get_grupo_local(os.Local_Empresa)
        is_hc = False
        
        if cliente_grupo.upper() == 'HOSPITAL DE CLINICAS':
            is_hc = True
        else:
            try:
                for tarefa in os.tarefas.all(): 
                    if tarefa.Ativo and 'HOSPITAL DE CLINICAS' in tarefa.Ativo.upper():
                        is_hc = True
                        break
            except Exception: 
                pass 

        if is_hc: cliente_grupo = 'Hospital De Clinicas' 

        # Técnicos e Tarefas
        tecnicos_set = set()
        tipos_tarefa_set = set()
        ativos_set = set()
        is_preventiva = False 

        try:
            for tarefa in os.tarefas.all(): 
                if tarefa.Responsavel:
                    tecnicos_set.add(get_grupo_tecnico(tarefa.Responsavel))
                if tarefa.Tipo_de_Tarefa:
                    tipos_tarefa_set.add(get_grupo_tipo_tarefa(tarefa.Tipo_de_Tarefa))
                    if 'Preventiva' in tarefa.Tipo_de_Tarefa:
                        is_preventiva = True
                if tarefa.Ativo:
                    ativos_set.add(tarefa.Ativo)
        except Exception:
            pass

        tecnico_resp = ", ".join(sorted(list(tecnicos_set)))
        tipo_tarefa = ", ".join(sorted(list(tipos_tarefa_set)))
        ativos_str = " | ".join(sorted(list(ativos_set)))

        # 1. Tempo de Atendimento 
        tempo_atendimento_td = None
        if os.Data_Iniciou_OS and os.Data_Criacao_OS:
            tempo_atendimento_td = os.Data_Iniciou_OS - os.Data_Criacao_OS

        # 2. Tempo em Execução 
        tempo_em_execucao_td = None
        if os.Data_Enviado_Verificacao and os.Data_Iniciou_OS:
            tempo_em_execucao_td = os.Data_Enviado_Verificacao - os.Data_Iniciou_OS

        # 3. Tempo em Verificação
        tempo_em_verificacao_td = None
        if os.Data_Finalizacao_OS and os.Data_Enviado_Verificacao:
            tempo_em_verificacao_td = os.Data_Finalizacao_OS - os.Data_Enviado_Verificacao

        tempo_total_para_sla = None
        if os.Data_Enviado_Verificacao and os.Data_Criacao_OS:
            tempo_total_para_sla = os.Data_Enviado_Verificacao - os.Data_Criacao_OS
        
        # SLAs
        sla_atendido = ""
        sla_violado = ""
        
        if not is_preventiva and tempo_total_para_sla:
            if cliente_grupo == 'Gerdau':
                ativo_code = None
                try:
                    tarefa_ativos = [t.Ativo for t in os.tarefas.all() if t.Ativo]
                    if any('(C0)' in a for a in tarefa_ativos): ativo_code = '(C0)'
                    elif any('(C1)' in a for a in tarefa_ativos): ativo_code = '(C1)'
                    elif any('(C2)' in a for a in tarefa_ativos): ativo_code = '(C2)'
                except Exception:
                    pass
                sla_hours_map = {'(C0)': 8, '(C1)': 24, '(C2)': 48}
                if ativo_code and ativo_code in sla_hours_map:
                    if tempo_total_para_sla <= timedelta(hours=sla_hours_map[ativo_code]):
                        sla_atendido = "Sim"; sla_violado = "Não"
                    else:
                        sla_atendido = "Não"; sla_violado = "Sim"
            
            elif cliente_grupo == 'Park Shopping Canoas':
                if tempo_total_para_sla <= timedelta(hours=24):
                    sla_atendido = "Sim"; sla_violado = "Não"
                else:
                    sla_atendido = "Não"; sla_violado = "Sim"
            
            elif cliente_grupo == 'Cpfl':
                if tempo_total_para_sla <= timedelta(hours=72):
                    sla_atendido = "Sim"; sla_violado = "Não"
                else:
                    sla_atendido = "Não"; sla_violado = "Sim"

        # Descrições 
        types_desc_str = ""
        causes_desc_str = ""
        detection_desc_str = ""

        try:
            for tarefa in os.tarefas.all():
                # Coleta dados acumulativos
                if tarefa.Responsavel:
                    tecnicos_set.add(get_grupo_tecnico(tarefa.Responsavel))
                if tarefa.Tipo_de_Tarefa:
                    tipos_tarefa_set.add(get_grupo_tipo_tarefa(tarefa.Tipo_de_Tarefa))
                    if 'Preventiva' in tarefa.Tipo_de_Tarefa: is_preventiva = True
                if tarefa.Ativo:
                    ativos_set.add(tarefa.Ativo)
                
                if not types_desc_str and tarefa.types_description:
                    types_desc_str = tarefa.types_description.strip()
                
                if not causes_desc_str and tarefa.causes_description:
                    causes_desc_str = tarefa.causes_description.strip()
                
                if not detection_desc_str and tarefa.detection_method_description:
                    detection_desc_str = tarefa.detection_method_description.strip()

        except Exception: pass
        
        row = {
            'OS': os.OS,
            'Status': os.Status,
            'Cliente': cliente_grupo,
            'Tecnico_Responsavel': tecnico_resp,
            'Ativo': ativos_str,
            'Descricao_OS': os.Observacao_OS,
            'Tipo_de_Tarefa': tipo_tarefa,
            'Descricao_Tipo': types_desc_str,
            'Descricao_Causa': causes_desc_str,
            'Metodo_Deteccao': detection_desc_str,
            'Data_Criacao': fmt_dt(os.Data_Criacao_OS),
            'Data_Inicio': fmt_dt(os.Data_Iniciou_OS),
            'Data_Conclusao_Tecnica': fmt_dt(os.Data_Enviado_Verificacao),
            'Data_Finalizacao': fmt_dt(os.Data_Finalizacao_OS),
            'Avanco_pct': os.Avanco_da_OS if os.Avanco_da_OS is not None else 0,
            
            # Usando as variáveis calculadas acima
            'Tempo_de_Atendimento': format_excel_timedelta(tempo_atendimento_td),
            'Tempo_em_Execucao': format_excel_timedelta(tempo_em_execucao_td),
            'Tempo_em_Verificacao': format_excel_timedelta(tempo_em_verificacao_td),
            
            'SLA_Atendido': sla_atendido,
            'SLA_Violado': sla_violado
        }
        linhas.append(row)

    tipo_relatorio_display = ''
    if tipo_relatorio:
        if tipo_relatorio == 'plano_tarefas_agrupados': tipo_relatorio_display = 'Ocorrências por Grupo de Plano de Tarefas'
        elif tipo_relatorio == 'tecnico_agrupados': tipo_relatorio_display = 'Ocorrências por Grupo de Técnico'
        else: tipo_relatorio_display = tipo_relatorio.replace("_", " ").title()

    context = {
        'linhas': linhas,
        'total': len(linhas),
        'tipo_relatorio': tipo_relatorio,
        'tipo_relatorio_display': tipo_relatorio_display,
        'categoria_display': categoria if categoria != 'todas' else 'Todas as Categorias',
        'start_date': start_date,
        'end_date': end_date,
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

    dados_filtrados = _get_dados_filtrados(
        tipo_relatorio, categoria, start_date, end_date
    ).prefetch_related('tarefas')

    if not dados_filtrados.exists():
        return HttpResponse("Não foram encontrados dados para os filtros e datas selecionados.")

    def fmt_dt(dt):
        try: 
            return localtime(dt).strftime('%d/%m/%Y %H:%M:%S') if dt else ''
        except Exception: 
            return str(dt) if dt else ''

    dados_para_excel = []

    for os in dados_filtrados:
        cliente_grupo = get_grupo_local(os.Local_Empresa)
        is_hc = False
        if cliente_grupo.upper() == 'HOSPITAL DE CLINICAS': is_hc = True
        else:
            try:
                for tarefa in os.tarefas.all(): 
                    if tarefa.Ativo and 'HOSPITAL DE CLINICAS' in tarefa.Ativo.upper():
                        is_hc = True; break
            except Exception: pass 
        if is_hc: cliente_grupo = 'Hospital De Clinicas' 

        # Técnicos, Ativos e Tipos
        tecnicos_set = set()
        tipos_tarefa_set = set()
        ativos_set = set()
        is_preventiva = False 

        types_desc_str = ""
        causes_desc_str = ""
        detection_desc_str = ""

        try:
            for tarefa in os.tarefas.all(): 
                # Coleta acumulativa
                if tarefa.Responsavel: tecnicos_set.add(get_grupo_tecnico(tarefa.Responsavel))
                if tarefa.Tipo_de_Tarefa:
                    tipos_tarefa_set.add(get_grupo_tipo_tarefa(tarefa.Tipo_de_Tarefa))
                    if 'Preventiva' in tarefa.Tipo_de_Tarefa: is_preventiva = True
                if tarefa.Ativo:
                    ativos_set.add(tarefa.Ativo)
                
                # LÓGICA DA DESCRIÇÃO 
                if not types_desc_str and tarefa.types_description: 
                    types_desc_str = tarefa.types_description.strip()
                
                if not causes_desc_str and tarefa.causes_description: 
                    causes_desc_str = tarefa.causes_description.strip()
                
                if not detection_desc_str and tarefa.detection_method_description: 
                    detection_desc_str = tarefa.detection_method_description.strip()

        except Exception: pass
            
        tecnico_resp = ", ".join(sorted(list(tecnicos_set)))
        tipo_tarefa = ", ".join(sorted(list(tipos_tarefa_set)))
        ativos_str = " | ".join(sorted(list(ativos_set)))

        # Tempos
        tempo_atendimento_td = None
        if os.Data_Iniciou_OS and os.Data_Criacao_OS:
            tempo_atendimento_td = os.Data_Iniciou_OS - os.Data_Criacao_OS

        tempo_em_execucao_td = None
        if os.Data_Enviado_Verificacao and os.Data_Iniciou_OS:
            tempo_em_execucao_td = os.Data_Enviado_Verificacao - os.Data_Iniciou_OS

        tempo_em_verificacao_td = None
        if os.Data_Finalizacao_OS and os.Data_Enviado_Verificacao:
            tempo_em_verificacao_td = os.Data_Finalizacao_OS - os.Data_Enviado_Verificacao
        
        tempo_total_para_sla = None
        if os.Data_Enviado_Verificacao and os.Data_Criacao_OS:
            tempo_total_para_sla = os.Data_Enviado_Verificacao - os.Data_Criacao_OS

        # SLA 
        sla_atendido = ""
        sla_violado = ""
        if not is_preventiva and tempo_total_para_sla:
            if cliente_grupo == 'Gerdau':
                ativo_code = None
                try:
                    tarefa_ativos = [t.Ativo for t in os.tarefas.all() if t.Ativo]
                    if any('(C0)' in a for a in tarefa_ativos): ativo_code = '(C0)'
                    elif any('(C1)' in a for a in tarefa_ativos): ativo_code = '(C1)'
                    elif any('(C2)' in a for a in tarefa_ativos): ativo_code = '(C2)'
                except Exception: pass
                sla_hours_map = {'(C0)': 8, '(C1)': 24, '(C2)': 48}
                if ativo_code and ativo_code in sla_hours_map:
                    if tempo_total_para_sla <= timedelta(hours=sla_hours_map[ativo_code]): sla_atendido = "Sim"; sla_violado = "Não"
                    else: sla_atendido = "Não"; sla_violado = "Sim"
            elif cliente_grupo == 'Park Shopping Canoas':
                if tempo_total_para_sla <= timedelta(hours=24): sla_atendido = "Sim"; sla_violado = "Não"
                else: sla_atendido = "Não"; sla_violado = "Sim"
            elif cliente_grupo == 'Cpfl':
                if tempo_total_para_sla <= timedelta(hours=72): sla_atendido = "Sim"; sla_violado = "Não"
                else: sla_atendido = "Não"; sla_violado = "Sim"

        row = {
            'OS': os.OS,
            'Status': os.Status,
            'Cliente': cliente_grupo,
            'Tecnico Responsavel': tecnico_resp,
            'Ativo': ativos_str,
            'Tipo de Tarefa': tipo_tarefa,
            'Descrição da OS': os.Observacao_OS,
            
            'Descrição do Tipo': types_desc_str,
            'Descrição da Causa': causes_desc_str,
            'Método de Detecção': detection_desc_str,
            
            'Data Criação': fmt_dt(os.Data_Criacao_OS),
            'Data Início': fmt_dt(os.Data_Iniciou_OS),
            'Data Conclusão Técnica': fmt_dt(os.Data_Enviado_Verificacao),
            'Data Finalização': fmt_dt(os.Data_Finalizacao_OS),
            'Avanço (%)': os.Avanco_da_OS if os.Avanco_da_OS is not None else 0,
            'Tempo de Atendimento': format_excel_timedelta(tempo_atendimento_td),
            'Tempo em Execução': format_excel_timedelta(tempo_em_execucao_td),
            'Tempo em Verificação': format_excel_timedelta(tempo_em_verificacao_td),
            'SLA Atendido': sla_atendido,
            'SLA Violado': sla_violado
        }
        dados_para_excel.append(row)

    df = pd.DataFrame(dados_para_excel)

    colunas_finais_ordenadas = [
        'OS', 'Status', 'Cliente', 
        'Tecnico Responsavel', 'Ativo', 'Tipo de Tarefa',
        'Descrição da OS', 
        'Descrição do Tipo', 'Descrição da Causa', 'Método de Detecção',
        'Data Criação', 'Data Início', 'Data Conclusão Técnica', 'Data Finalização', 'Avanço (%)',
        'Tempo de Atendimento', 'Tempo em Execução', 'Tempo em Verificação',
        'SLA Atendido', 'SLA Violado'
    ]
    
    colunas_finais = [col for col in colunas_finais_ordenadas if col in df.columns]

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Relatorio', columns=colunas_finais)
    
    buffer.seek(0)
    response = HttpResponse(buffer, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = f'attachment; filename="relatorio_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx"'
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
        todos_locais = sorted([kw.title() for kw in KEYWORDS_LOCAIS])
        gerdau_especificos = [l for l in todos_locais if l.upper().startswith('GERDAU')]
        outros_locais = [l for l in todos_locais if not l.upper().startswith('GERDAU')]
        categorias.append('Gerdau')
        categorias.extend(sorted(gerdau_especificos))
        categorias.append('Gerdau Outros')
        categorias.extend(sorted(outros_locais))
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

def api_detalhes_tecnico(request):
    tecnico = request.GET.get('tecnico')
    start_date = request.GET.get('start_date')
    end_date = request.GET.get('end_date')
    local_grupo = request.GET.get('local_grupo')

    dados_qs = _get_dados_filtrados(
            'tecnico_agrupados', 
            tecnico, 
            start_date, 
            end_date, 
            filtro_local_extra=local_grupo
        ).prefetch_related('tarefas')
    
    def fmt_dt(dt):
        return localtime(dt).strftime('%d/%m/%Y %H:%M') if dt else ''

    def fmt_td(td):
        if not td or not isinstance(td, timedelta): return ''
        total_seconds = int(td.total_seconds())
        days, remainder = divmod(total_seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)
        if days > 0: return f'{days}d {hours:02}:{minutes:02}'
        return f'{hours:02}:{minutes:02}'

    resultados = []
    
    for os in dados_qs:
        #  LÓGICA DE DADOS 
        cliente_grupo = get_grupo_local(os.Local_Empresa)
        if cliente_grupo.upper() == 'HOSPITAL DE CLINICAS': cliente_grupo = 'Hospital De Clinicas'
        
        tecnicos_set = set()
        ativos_set = set()
        tipos_tarefa_set = set()
        
        for t in os.tarefas.all():
            if t.Responsavel: tecnicos_set.add(t.Responsavel) 
            if t.Ativo: ativos_set.add(t.Ativo)
            if t.Tipo_de_Tarefa: tipos_tarefa_set.add(t.Tipo_de_Tarefa)

        # Cálculos de tempo básicos
        tempo_atendimento = fmt_td(os.Data_Iniciou_OS - os.Data_Criacao_OS) if (os.Data_Iniciou_OS and os.Data_Criacao_OS) else ''
        tempo_resolucao = fmt_td(os.Data_Enviado_Verificacao - os.Data_Criacao_OS) if (os.Data_Enviado_Verificacao and os.Data_Criacao_OS) else ''

        resultados.append({
            'os': os.OS,
            'status': os.Status,
            'cliente': cliente_grupo,
            'tecnicos': ", ".join(tecnicos_set),
            'ativo': " | ".join(ativos_set),
            'tipo_tarefa': ", ".join(tipos_tarefa_set),
            'descricao': os.Observacao_OS or "",
            'data_criacao': fmt_dt(os.Data_Criacao_OS),
            'data_inicio': fmt_dt(os.Data_Iniciou_OS),
            'data_fim': fmt_dt(os.Data_Enviado_Verificacao),
            'tempo_atendimento': tempo_atendimento,
            'tempo_resolucao': tempo_resolucao
        })

    return JsonResponse({'dados': resultados})