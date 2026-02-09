from django.db import models
import unicodedata
import re
import difflib

TRT_SETORES_POA = {
    "Almoxarifado": "Almoxarifado POA",
    "Arquivo e Memorial": "Arquivo e Memorial POA",
    "Cadastramento de bens": "Cadastramento de bens POA",
    "Marcenaria": "Marcenaria POA",
    "Matriz Setese": "Matriz Setese POA",
    "Sede": "Sede POA",
    "Transportes": "Transportes POA",
    "Triagem": "Triagem e Depósito POA",
    "Foro Trabalhista Porto Alegre": "Foro Trabalhista POA"
}


TRT_CIDADES = [
    "Bagé", "Bento Gonçalves", "Cachoeirinha", "Canoas", "Caxias do Sul", "Erechim", "Estrela", 
    "Gramado", "Gravataí", "Novo Hamburgo", "Passo Fundo", "Pelotas", "Rio Grande", 
    "Santa Cruz do Sul", "Santa Maria", "Santa Rosa", "Sapiranga", "Sapucaia do Sul", 
    "São Leopoldo", "Taquara", "Uruguaiana", "Capão da Canoa", "Dom Pedrito", "Itaqui", 
    "Marau", "Nova Prata", "Panambi", "São Lourenço do Sul", "São Sebastião do Cai", 
    "Taquari", "Tramandaí", "Frederico Westphalen", "Alegrete", "Alvorada", "Arroio Grande", 
    "Cachoeira do Sul", "Camaquã", "Carazinho", "Cruz Alta", "Encantado", "Esteio", 
    "Estância Velha", "Farroupilha", "Guaíba", "Ijuí", "Lagoa Vermelha", "Lajeado", 
    "Montenegro", "Osório", "Palmeira das Missões", "Rosário do Sul", "Santa Vitória do Palmar", 
    "Santana do Livramento", "Santiago", "Santo Ângelo", "Soledade", "São Borja", 
    "São Gabriel", "São Jerônimo", "Torres", "Triunfo", "Três Passos", "Vacaria", "Viamão"
]

KEYWORDS_LOCAIS = [
    "ADAMA", "ADM BRASIL", "ALPHAVILLE", "ARCELORMITTAL", "ASSEMBLEIA", "BALL", "BIC", "CPFL", "CSN",
    "ENGELOG", "FITESA", "FUNDAÇÃO BANRISUL", "HOSPITAL DE CLINICAS", "LOJA MAÇONICA", "M DIAS", "NEOENERGIA",
    "PORTO DO AÇU", "RAIZEN", "SICOOB", "SIMEC", "SUMESA", "TRÊS CORAÇÕES", "TRT", "TURIS", "UNILEVER",
    "UFRGS IPH", "UFRGS", "PARK SHOPPING CANOAS", "CANOAS", "TRE", "Pandrol", "Gerdau  Tijucas", "Gerdau Araucária", "Gerdau Araçariguama"
    ,"Gerdau Açonorte", "Gerdau Aços Longos S.A", "Gerdau Barão", "Gerdau Caucaia", "Gerdau Cearense", "Gerdau Charqueadas", "Gerdau Corte e Dobra Armafer"
    ,"Gerdau Corte e Dobra Chapecó", "Gerdau Corte e Dobra Suape", "Gerdau Cosigua", "Gerdau Curitiba", "Gerdau Divinópolis", "Gerdau Mogi das Cruzes"
    ,"Gerdau Ouro Branco", "Gerdau Pindamonhangaba", "Gerdau Praia Mole", "Gerdau Sapucaia", "Gerdau São José"
]

MAPEAMENTO_PLANO_TAREFAS_DETALHADO = {
  "Corretiva": [
    "Corretiva",
    "Corretiva Teste",
    "Corretiva com análise de falha",
    "Corretivo Cartão SD CARD",
    "Corretivo de Caixa Econômica Federal",
    "Correção de falha",
    "Plano Corretivo Cliente Contrato",
    "Plano Corretivo Cliente Contrato Emergencial",
    "Manutenção Corretiva do Equipamentos de Alarme e C",
    "Manutenção Corretiva do Equipamentos de CFTV",
    "Manutenção Corretivo Cliente avulso",
    "Manutenção de Garantia da Implantação",
    "Acesso Remoto",
    "Acompanhamento Técnico",
    "Acompanhamento de atividade de terceiros",
    "Apoio Técnico CFTV Contrato",
    "Telefone de Emergência",
    "Usuário Sistema Gitel",
    "Criar Usuário",
    "Criação de ádio para central tel",
    "Reunião CSN SIRENE",
    "Levantamento para Manutenção",
    "Levantamento para Manutenção Avulso",
    "Levantamento para Manutenção Contrato Seg.Eletroni",
    "Levantamento para Manutenção Contrato Telefonia",
    "Levantamento para Manutenção de Contrato",
    "Manutenção Remota",
    "Manutenção Remota CFTV Avulso",
    "Manutenção Remota CFTV Contrato",
    "Manutenção Remota Telefonia Avulso",
    "Manutenção Remota Telefonia Contrato",
    "Manutenção Remota de Clientes Avulso",
    "Manutenção Remota de Clientes de Contrato",
    "Manutenção Chamado Avulso",
    "Manutenção de Chamado Contrato",
    "Substituição",
    "Substituição de Equipamento",
    "Troca de equipamento",
    "Equipamento para Teste",
    "Remanejamento",
    "Reposicionamento",
    "Devolução",
    "Retirada de Equipamento",
    "Retirada de equipamento para manutenção",
    "RETIRADA",
    "Termo de Aceite",
    "Procedimento",
    "Garantia",
    "QDV",
    "Correto",
    "Atividades Extras",
    "Orçamento",
    "Exportação de Imagens Cliente de Contrato",
    "Recusa de Tarefa",
    "Teste",
    "Configuração Sirene"
  ],

  "Preventiva": [
    "Autorização Preventiva",
    "Preventiva",
    "Preventiva Câmeras",
    "Preventiva Neo",
    "Plano Preventivo Cliente Contrato",
    "Manutenção Preventiva CFTV",
    "Manutenção Preventiva do Equipamentos de REDE - RA",
    "Programação de Manutenção",
    "Programação de Manutenção de Segurança Eletrônica",
    "Programação de Manutenção de Telefônica",
    "Programações",
    "POC",
    "CDM",
    "Analítico",
    "Checklist Servidor",
    "checklist",
    "Limpeza de Equipamentos",
    "Laudo",
    "Treinamento"
  ],

  "Instalação": [
    "Instalação",
    "Instalação Manutenção",
    "Instalação Manutenção Avulso",
    "Instalação de novos serviços",
    "Instalação e manutenção de Software",
    "Instalação novos serviços CFTV Contrato",
    "Instalação para Implantação",
    "Instalação/Manutenção de Software",
    "PO Instalação de Novos Serviços",
    "Plano de Instalação Termica",
    "Corretiva/Preventiva/Instalação",
    "Levantamento",
    "Levantamento para Implantação",
    "Instalação de Licenças",
    "Entrega",
    "Entrega de documentação",
    "Entrega de material",
    "Fornecimento de Mão de Obra - Projetos"
  ]
}

MAPEAMENTO_TECNICOS = {
  "Adair Breitkreitz": [
    "Adair Breitkreitz"
  ],
  "Adones/Nilson - Equipe Cosigua": [
    "Adones/Nilson - Equipe Cosigua"
  ],
  "Afirma Sistemas de Segurança": [
    "Afirma Sistemas de Segurança - Giane e Fabio"
  ],
  "Afonso Junior": [
    "Afonso Junior"
  ],
  "Agnaldo": [
    "Agnaldo - Equipe Shopping", "Agnaldo Santos", "Agnaldo/Felipe - Equipe Shopping"
  ],
  "Airton Furtado": [
    "Airton Furtado"
  ],
  "Alcides/Marcelo Aço Norte": [
    "Alcides Junior", "Alcides Junior Gerdau Aço Norte", "Alcides/Marcelo - Equipe Aço Norte"
  ],
  "Alex Romanato Charqueadas": [
    "Alex Romanato", "Alex Romanato Gerdau Charqueadas"
  ],
  "Alex/Ygor - Equipe Charqueadas": [
    "Alex/Ygor - Equipe Charqueadas"
  ],
  "Alexandre Hermogenes": [
    "Alexandre Hermogenes", "Alexandre Hermogenes Gerdau Cosigua"
  ],
  "Alexandre Rutkoski": [
    "Alexandre Rutkoski"
  ],
  "Alisson Camargo": [
    "Alisson Camargo/Luis Gustavo Equipe 2"
  ],
  "Allysson Cearense": [
    "Allysson - Cearense", "Allysson - Equipe Cearense"
  ],
  "Anderson Souza NOC": [
    "Anderson Souza", "Anderson Souza - NOC", "Anderson Souza Gerdau Cearense", "Anderson Souza Gerdau Sapucaia"
  ],
  "Anderson Domingues": [
    "Anderson Domingues - NOC", "Anderson Domingues Oliveira", "Anderson Oliveira"
  ],
  "Anderson Arnaldo Baldini": [
    "Anderson Arnaldo Baldini"
  ],
  "André/Daniel Equipe Alphaville": [
    "Andre Darós", "Andre Rodrigo Farinhas Daros CPF:03188419082", "Andre daros", "André/Daniel Equipe Alphaville", "André/Lucas - Equipe Alphaville", "André/Nimay - Equipe Alphaville"
  ],
  "Artur Melo": [
    "Artur Melo CPF:044.247.690-62", "Artur Melo/Luis Gustavo - Equipe 2"
  ],
  "Augusto Brum": [
    "Augusto - Equipe 1", "Augusto Brum", "Augusto Brum - Equipe 1", "Augusto Santos de Brum CPF:03494672008", "Augusto Santos de Brum CPF:0394672008", "Augusto/Gustavo - Equipe 1", "Augusto/Gustavo - Equipe Alfa"
  ],
  "Breno/Juan Pinda": [
    "Breno /Juan Equipe Pinda 4", "Caio/Breno - Equipe Pinda 3", "Marcelo/Breno - Equipe Pinda 3"
  ],
  "Caio": [
    "Caio Raybbot Gerdau Pinda",  "Caio/Hayan - Equipe Pinda 3", "Caio/Itamar - Equipe Pinda 3"
  ],
  "Carlos Augusto Gomes dos Santos": [
    "Carlos Augusto Gomes dos Santos"
  ],
  "Carlos Rezes da Silva": [
    "Carlos Rezes da Silva"
  ],
  "Christian de Sá": [
    "Christian de Sá Gerdau Sapucaia"
  ],
  "Chrystian Menegat Muller": [
    "Chrystian Menegat Muller"
  ],
  "Cleber Campos": [
    "Cleber Campos", "Cleber Campos CPF:830.646.990.91"
  ],
  "Coringa": [
    "Coringa"
  ],
  "Cristiano Pires Me": [
    "Cristiano Pires Me"
  ],
  "Danilo Passeri Leal": [
    "Danilo Passeri Leal"
  ],
  "Debora Alves": [
    "Debora alves"
  ],
  "Dennis Nogueira": [
    "Dennis Nogueira"
  ],
  "Dilma Heber": [
    "Dilma Heber"
  ],
  "Douglas Medeiros": [
    "Douglas Medeiros"
  ],
  "Douglas Moura Correa": [
    "Douglas Moura Correa"
  ],
  "Eder": [
    "Eder - Equipe Mogi", "Eder Henrique Pereira Gerdau Mogi", "Eder/Alan - Equipe Mogi"
  ],
  "Edilson Junior": [
    "Edilson Junior", "Edilson Junior Gerdau GSP"
  ],
  "Eduardo Weck": [
    "Eduardo Weck", "Eduardo Weck Gerdau GSP"
  ],
  "Eider Bottcher Coelho": [
    "Eider Bottcher Coelho", "Eider Bottcher Coelho Projeto Gerdau"
  ],
  "Elias Conceição": [
    "Elias conceição", "Elias dos Santos Conceição CPF:000.572.160-10"
  ],
  "Evandro Antonio Adanski": [
    "Evandro Antonio Adanski"
  ],
  "Everaldo": [
    "Everaldo - CSN", "Everaldo Aparecido de Assis"
  ],
  "Everton Pereira Soares": [
    "Everton Pereira Soares"
  ],
  "Fabiano Barbosa Ferreira": [
    "Fabiano Barbosa Ferreira"
  ],
  "Fabricio - Gilvan Equipe Cosigua 2": [
    "Fabricio - Equipe Cosigua 2", "Fabricio - Gilvan Equipe Cosigua 2"
  ],
  "Felipe": [
    "Felipe - Equipe Sapucaia", "Felipe Augusto", "Felipe Corletto CPF:01800206097"
  ],
  "Fernando": [
    "Fernando Albuquerque", "Fernando Henrique de Lima"
  ],
  "Francisco Cearense": [
    "Francisco - Equipe Cearense", "Francisco Freitas Gerdau Cearense", "Francisco José Volonte Rodrigues", "Francisco/ Allysson - Equipe Cearense"
  ],
  "Fredi Ervino Guthoff": [
    "Fredi", "Fredi Ervino Guthoff"
  ],
  "Gabriel": [
    "Gabriel Alves Gerdau Cosigua", "Gabriel/Leonardo - Equipe Cosigua 2", "Gabriel/Washington - Equipe Cosigua 2"
  ],
  "Gilson Elcio Barreira Vianna": [
    "Gilson Elcio Barreira Vianna"
  ],
  "Gustavo": [
    "Gustavo / Brian", "Gustavo Arantes Gerdau GSP", "Gustavo teixeira"
  ],
  "Helio Da costa Guilherme": [
    "Helio Da costa Guilherme"
  ],
  "Igor Ferreira": [
    "Igor Ferreira", "Igor - Equipe Cosigua 2", "Igor / Gabriel - Equipe Cosigua 2", "Igor Ferreira Gerdau GSP"
  ],
  "Irwyrn Gonzaga": [
    "Irwyrn Gonzaga", "Irwyrn Gonzaga Gerdau Cosigua"
  ],
  "Jair": [
    "Jair Equipe 3", "Jair Muhlbeier", "Jair/Rui - Equipe 3"
  ],
  "Jeferson Bottcher": [
    "Jeferson Bottcher", "Jeferson Bottcher Coelho CPF:019.571.080-02"
  ],
  "Julio Castro": [
    "Julio Castro", "Julio Castro Gerdau Cearense"
  ],
  "Leandro Morgado": [
    "Leandro Morgado Silva",
  ],
  "Leandro mineiro": [
    "Leandro mineiro"
  ],
  "Leonardo César": [
    "Leonardo César", "Leonardo César CSN Itaguaí"
  ],
  "Luis Gustavo": [
    "Luis Gustavo Equipe 2", "Luis Gustavo Gerdau Pinda", "Luis Gustavo Silva", "Luis Gustavo de Campos", "Luiz Gustavo Medeiros da Silva CPF:676.007.860.87"
  ],
  "Marcelo Pinda 3": [
    "Marcelo/Brian Equipe Pinda 3", "Marcelo/Hayan - Equipe Pinda 3", "Marcelo/Luis Henrique Equipe Pinda 3"
  ],
  "Marcelo Oliveira": [
    "Marcelo Oliveira", "Marcelo Oliveira Gerdau Recife"
  ],
  "Marcelo Coelho": [
    "Marcelo Coelho"
  ],
  "Marcelo Rojo CSN Congonhas CFTV": [
    "Marcelo Rojo CSN Congonhas CFTV"
  ],
  "Marcio Amaral": [
    "Marcio Amaral",
  ],
  "Marcio Edvil": [
    "Marcio Edvil"
  ],
  "Marcus Vinicius Vieira dos Santos": [
    "Marcus Vinicius", "Marcus Vinicius Vieira dos Santos"
  ],
  "Misael Fagundes": [
    "Misael Fagundes"
  ],
  "Nimay Souza": [
    "Nimay Souza"
  ],
  "Paulo": [
    "Paulo Caner CPF:327.250.738.80", "Paulo Garretano Junior", "Paulo Henrique Fogaça"
  ],
  "Rafael Vieira": [
    "Rafael CPF:007.859.680-75 Vieira", "Rafael Vieira", "Rafael Vieira CPF:007.859.680-75"
  ],
  "Reynaldo Conte": [
    "Reynaldo - Equipe Pinda", "Reynaldo - LiderSit", "Reynaldo Conde Gerdau Pinda", "Reynaldo Conte", "Reynaldo Supervisor", "Reynaldo. Conte", "Reynaldo/Brain LiderSit", "Reynaldo/Brian LiderSit", "Reynaldo/Luis Henrique LiderSit", "Reynaldo/Marcelo - Equipe Pinda"
  ],
  "Joelson -": [
    "Ricardo Sousa Gerdau GSP", "Ricardo/Joelson - Equipe GSP", "Ricardo/Tiago - Equipe GSP", "Joelson - Equipe GSP 2", "Joelson - TESTE", "Joelson -"
  ],
  "Rodrigo Ramos": [
    "Rodrigo Ramos"
  ],
  "Sandro Silva": [
    "Sandro Silva Gerdau Pinda"
  ],
  "Sergio": [
    "Sergio Gil"
  ],
  "Sergio Pivetta": [
    "Sergio Pivetta"
  ],
  "Tainan Ribeiro": [
    "Tainan - PCM", "Tainan Ribeiro"
  ],
  "Tiago": [
    "Tiago Alberto da Costa"
  ],
  "Tiago Bayer Casanova - Fatec": [
    "Tiago Bayer Casanova - Fatec"
  ],
  "Vagner Costa": [
    "Vagner - Equipe Divinópolis", "Vagner Costa", "Vagner Costa Equipe Divinópolis", "Vagner Costa Gitel Sede", "Vagner/ André - Equipe Divinópolis", "Vagner/ Igor - Equipe Divinópolis", "Vagner/Nathanael - Equipe Divinópolis"
  ],
  "Wilsen Tony": [
    "Willian Wilsen Tony Pereira", "Wilsen Tony Pereira Gerdau Pinda"
  ],
  "Wesley de Oliveira": [
    "Wesley de Oliveira"
  ],
  "Claudio BUS - Cosigua": [
    "Claudio BUS - Cosigua"
  ],
  "Osvaldo - CSN Minas": [
    "Osvaldo - CSN Minas"
  ],
  "Tadeu Oliveira": [
    "Tadeu Oliveira"
  ],
  "Nathan Fraga - NOC": [
    "Nathan Fraga - NOC"
  ],
  "Fernades/João Sapucaia": [
    "Fernades/João - Equipe Sapucaia"
  ],
  "Jonathan Freitas": [
    "Jonathan Freitas"
  ],
}

def normalize_text(s: str) -> str:
    """Normaliza strings removendo acentos e caracteres especiais."""
    if not s:
        return ''
    s = unicodedata.normalize('NFKD', str(s)) 
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
    if not responsavel_str: return 'Não Mapeado'
    norm = normalize_text(responsavel_str)
    if norm in TECNICO_PARA_GRUPO_MAP: return TECNICO_PARA_GRUPO_MAP[norm]
    for key in KNOWN_TECNICO_KEYS:
        if key and key in norm: return TECNICO_PARA_GRUPO_MAP[key]
    close = difflib.get_close_matches(norm, KNOWN_TECNICO_KEYS, n=1, cutoff=0.85)
    return TECNICO_PARA_GRUPO_MAP[close[0]] if close else 'Outros'

def get_grupo_tipo_tarefa(tipo_str):
    if not tipo_str: return 'Não Categorizado'
    return TIPO_TAREFA_PARA_GRUPO.get(' '.join(tipo_str.strip().split()).lower(), 'Outros')

def get_grupo_local(local_str):
    """Retorna o GRUPO macro (Ex: Tudo que é Gerdau vira 'Gerdau')"""
    if not local_str: return 'Outros'
    local_upper = local_str.upper()

    if 'TRT' in local_upper or '4 REGIAO' in local_upper or 'TRT4' in local_upper:
        return 'TRT'
    
    if 'GERDAU' in local_upper:
        return 'Gerdau'

    melhor_match, menor_indice = None, float('inf')
    for keyword in KEYWORDS_LOCAIS:
        indice = local_upper.find(keyword.upper())
        if indice != -1 and indice < menor_indice:
            menor_indice, melhor_match = indice, keyword.title()
            
    return melhor_match if melhor_match else 'Outros'

def get_trt_specific_name(text_str):
    if not text_str: return 'TRT Outros'
    
    text_norm = normalize_text(text_str) 
    
    for chave, valor_formatado in TRT_SETORES_POA.items():
        if normalize_text(chave) in text_norm:
            return f"TRT {valor_formatado}"
            
    for cidade in TRT_CIDADES:
        if normalize_text(cidade) in text_norm:
            return f"TRT {cidade}" 
            
    return 'TRT Outros'

def get_local_detalhado(local_str):
    """Retorna o local ESPECÍFICO (Ex: 'Gerdau Cearense' ou 'Gerdau Outros')"""
    if not local_str: return 'Outros'
    local_upper = local_str.upper()

    if 'TRT' in local_upper or '4 REGIAO' in local_upper or 'TRT4' in local_upper:
        return get_trt_specific_name(local_str)

    melhor_match, menor_indice = None, float('inf')
    for keyword in KEYWORDS_LOCAIS:
        indice = local_upper.find(keyword.upper())
        if indice != -1 and indice < menor_indice:
            menor_indice, melhor_match = indice, keyword.title()
    
    if melhor_match:
        return melhor_match

    if 'GERDAU' in local_upper:
        return 'Gerdau Outros'

    return 'Outros'