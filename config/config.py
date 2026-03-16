"""
config/config.py
----------------
Ponto único de configuração do pipeline.
Altere aqui as UASGs, endpoints, anos e parâmetros de comportamento.
"""

from datetime import datetime

# ---------------------------------------------------------------------------
# UNIDADES GESTORAS (UASGs)
# ---------------------------------------------------------------------------
UASGS = [
    {"sigla": "RT", "codigo": "158132", "nome": "IFMS REITORIA"},
    {"sigla": "AQ", "codigo": "158448", "nome": "IFMS CAMPUS AQUIDAUANA"},
    {"sigla": "CG", "codigo": "158449", "nome": "IFMS CAMPUS CAMPO GRANDE"},
    {"sigla": "CB", "codigo": "158450", "nome": "IFMS CAMPUS CORUMBA"},
    {"sigla": "CX", "codigo": "158451", "nome": "IFMS CAMPUS COXIM"},
    {"sigla": "DR", "codigo": "155848", "nome": "IFMS CAMPUS DOURADOS"},
    {"sigla": "JD", "codigo": "155850", "nome": "IFMS CAMPUS JARDIM"},
    {"sigla": "NA", "codigo": "158452", "nome": "IFMS CAMPUS NOVA ANDRADINA"},
    {"sigla": "NV", "codigo": "155849", "nome": "IFMS CAMPUS NAVIRAÍ"},
    {"sigla": "PP", "codigo": "158453", "nome": "IFMS CAMPUS PONTA PORÃ"},
    {"sigla": "TL", "codigo": "158454", "nome": "IFMS CAMPUS TRÊS LAGOAS"},
]

# ---------------------------------------------------------------------------
# APIs
# ---------------------------------------------------------------------------
BASE_URL = "https://dadosabertos.compras.gov.br"

CONFIG_APIS = {
    "LEGADO": {
        "base_url": BASE_URL,
        "pasta_cache": "temp/compras",      # pasta única para legado + PNCP
        "anos": list(range(2016, datetime.now().year + 1)),
        "uasgs": UASGS,
        # Configure False para pular extração Legado no pipeline padrão
        "executar_legado": False,
        "endpoints": [
            {
                "label": "outrasmodalidades",
                "path": "/modulo-legado/1_consultarLicitacao",
                "p_uasg": "uasg",
                "p_data": "data_publicacao",
            },
            {
                "label": "pregao",
                "path": "/modulo-legado/3_consultarPregoes",
                "p_uasg": "co_uasg",
                "p_data": "dt_data_edital",
            },
            {
                "label": "dispensa",
                "path": "/modulo-legado/5_consultarComprasSemLicitacao",
                "p_uasg": "co_uasg",
                "p_data": None,
            },
        ],
    },
    "LEI14133": {
        "base_url": BASE_URL,
        "pasta_cache": "temp/compras",      # mesma pasta — arquivos prefixados "pncp_"
        "anos": list(range(2021, datetime.now().year + 1)),
        "uasgs": [u for u in UASGS if u["sigla"] == "RT"],
        "modalidades": {
            3: "concorrencia",
            5: "pregao",
            6: "dispensa",
            7: "inexigibilidade",
        },
        "path": "/modulo-contratacoes/1_consultarContratacoes_PNCP_14133",
    },
}

# ---------------------------------------------------------------------------
# MODALIDADES — dicionário oficial (código int → nome padronizado)
# Usado pelos transformers para garantir nomes consistentes.
# ---------------------------------------------------------------------------
MODALIDADES: dict[int, str] = {
    1:  "Convite",
    2:  "Tomada de Preços",
    3:  "Concorrência",
    5:  "Pregão",
    6:  "Dispensa",
    7:  "Inexigibilidade",
    99: "RDC",
}

# Situações PNCP consideradas finais (não precisam ser re-consultadas)
SITUACOES_FINAIS_PNCP: set[int] = {3, 4, 5}

# ---------------------------------------------------------------------------
# ATAS DE REGISTRO DE PREÇO (ARP)
# Apenas a unidade gerenciadora (RT) realiza compras centralizadas.
# Filtramos por dataVigenciaInicial por ano e deduplicamos no transformer.
# Vigência máxima pela Lei 14.133: 1 ano + 1 ano de prorrogação.
# ---------------------------------------------------------------------------
CONFIG_ATAS = {
    "base_url":         BASE_URL,
    "path":             "/modulo-arp/1_consultarARP",
    "pasta_cache":      "temp/atas",
    "uasg":             {"sigla": "RT", "codigo": "158132"},
    "anos":             list(range(2023, datetime.now().year + 1)),

    # Itens das atas — janelas anuais (01/01 → 31/12) de 2023 em diante.
    # Cobre até ano_atual + 1 para capturar atas com prorrogação ainda vigentes.
    "pasta_cache_itens": "temp/atas_itens",
    "pasta_cache_saldos":    "temp/atas_saldos",
    "pasta_cache_unidades":  "temp/atas_unidades",
    # Configure False para pular extração de saldos das atas no pipeline completo
    "executar_saldos": False,
    "anos_itens":        list(range(2023, datetime.now().year + 2)),
}

# ---------------------------------------------------------------------------
# CONTRATOS (contratos.comprasnet.gov.br)
# API diferente da dadosabertos — retorna listas diretas sem paginação.
# ---------------------------------------------------------------------------
CONFIG_CONTRATOS = {
    "base_url":                  "https://contratos.comprasnet.gov.br/api",
    "pasta_cache":               "temp/contratos",
    "pasta_cache_responsaveis":  "temp/contratos_responsaveis",
    "uasgs":                     UASGS,
}

# ---------------------------------------------------------------------------
# EXPORTAÇÃO CSV (Power BI)
# ---------------------------------------------------------------------------
EXPORT_CONFIG = {
    "pasta_saida": "data",
    "encoding":    "utf-8-sig",   # BOM para compatibilidade com Excel/Power BI
    "separador":   ";",           # ponto-e-vírgula padrão para pt-BR
}

# ---------------------------------------------------------------------------
# COMPORTAMENTO DO PIPELINE
# ---------------------------------------------------------------------------
PIPELINE_CONFIG = {
    # Threads simultâneas por motor
    "max_workers_legado":         3,
    "max_workers_14133":          2,
    "max_workers_itens":          3,
    "max_workers_atas":           3,
    "max_workers_contratos":      5,
    "max_workers_responsaveis":  15,

    # Requisições HTTP
    "timeout_segundos":        30,
    # Timeout menor para a API legado (instável mas responde rápido quando está no ar)
    "timeout_segundos_legado": 10,
    # Timeout maior para saldos — endpoint mais lento
    "timeout_segundos_saldos": 120,
    "tamanho_pagina":      500,

    # Backoff exponencial (segundos)
    "backoff_inicial":          2,
    "backoff_tentativas":       2,      # 2 s → 4 s
    "backoff_tentativas_saldos": 6,     # mais tentativas para endpoint instável

    # Cache: dias antes de re-verificar contratos ainda em aberto (PNCP)
    "dias_validade_cache_pncp":          7,

    # Cache: dias antes de re-verificar atas (cabeçalho) para detectar prorrogações
    "dias_validade_cache_atas":          30,

    # Janela de alerta: re-verifica se há atas com vigência final nos próximos N dias
    "dias_alerta_prorrogacao_atas":      60,

    # Cache: contratos e responsáveis — dados que mudam com aditivos/rescisões
    "dias_validade_cache_contratos":      1,
    "dias_validade_cache_responsaveis":   1,

    # Log: a cada N skips imprime resumo (evita flood no terminal)
    "log_intervalo_skip":  50,
}

# ---------------------------------------------------------------------------
# HEADERS HTTP
# Simula navegador para evitar bloqueios anti-bot em endpoints sensíveis
# (ex: 4_consultarEmpenhosSaldoItem rejeita requests sem User-Agent)
# ---------------------------------------------------------------------------
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection":      "keep-alive",
}
