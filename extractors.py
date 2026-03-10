"""
extractors.py
-------------
Lógica de extração paginada para cada módulo da API.

Cada função processa uma combinação (unidade × ano × endpoint/modalidade),
percorrendo todas as páginas disponíveis e delegando cache e HTTP aos
módulos especializados.
"""

from config import PIPELINE_CONFIG, CONFIG_APIS
from cache import verificar_sucesso_anterior, deve_reverificar_pncp, salvar_dados
from api_client import consultar_api
from logger import log_skip, log_sucesso, log_falha


_TAMANHO_PAGINA = PIPELINE_CONFIG["tamanho_pagina"]


# ---------------------------------------------------------------------------
# Módulo LEGADO
# ---------------------------------------------------------------------------
def extrair_legado(unidade: dict, ano: int, endpoint: dict) -> bool:
    """
    Extrai dados do módulo legado para uma combinação (unidade, ano, endpoint).

    Retorna True se concluiu sem falhas, False se alguma página falhou.
    """
    cfg = CONFIG_APIS["LEGADO"]
    pagina = 1

    while True:
        caminho = _caminho_legado(
            cfg["pasta_cache"], endpoint["label"], unidade["sigla"], ano, pagina)
        ja_existe, dados_cache = verificar_sucesso_anterior(caminho)

        if ja_existe:
            respostas = dados_cache.get("respostas", {})
            log_skip(unidade["sigla"], endpoint["label"], ano)

            if _tem_mais_paginas(respostas):
                pagina += 1
                continue
            break

        params = _montar_params_legado(unidade, ano, endpoint, pagina)
        dados, status = consultar_api(
            f"{cfg['base_url']}{endpoint['path']}", params)
        salvar_dados(
            caminho, f"{cfg['base_url']}{endpoint['path']}", params, dados, status)

        if status == "SUCESSO":
            log_sucesso(unidade["sigla"], endpoint["label"], ano, pagina)
            if _tem_mais_paginas(dados):
                pagina += 1
                continue
            break
        else:
            log_falha(unidade["sigla"], endpoint["label"], ano, pagina)
            return False

    return True


# ---------------------------------------------------------------------------
# Módulo LEI 14133 (PNCP)
# ---------------------------------------------------------------------------
def extrair_14133(unidade: dict, ano: int, cod_modalidade: int, nome_modalidade: str) -> bool:
    """
    Extrai dados do módulo Lei 14.133/PNCP para uma combinação
    (unidade, ano, modalidade).

    Retorna True se concluiu sem falhas, False se alguma página falhou.
    """
    cfg = CONFIG_APIS["LEI14133"]
    pagina = 1
    label = f"PNCP-{nome_modalidade}"

    while True:
        caminho = _caminho_14133(
            cfg["pasta_cache"], unidade["sigla"], nome_modalidade, ano, pagina)
        ja_existe, dados_cache = verificar_sucesso_anterior(caminho)

        if ja_existe:
            respostas = dados_cache.get("respostas", {})
            precisa_reverificar = deve_reverificar_pncp(dados_cache)

            if not precisa_reverificar:
                log_skip(unidade["sigla"], label, ano)
                if _tem_mais_paginas(respostas) and respostas.get("resultado"):
                    pagina += 1
                    continue
                break
            # Se precisa reverificar, segue para a requisição abaixo

        params = _montar_params_14133(unidade, ano, cod_modalidade, pagina)
        dados, status = consultar_api(
            f"{cfg['base_url']}{cfg['path']}", params)
        salvar_dados(
            caminho, f"{cfg['base_url']}{cfg['path']}", params, dados, status)

        if status == "SUCESSO":
            log_sucesso(unidade["sigla"], label, ano, pagina)
            if _tem_mais_paginas(dados):
                pagina += 1
                continue
            break
        else:
            log_falha(unidade["sigla"], label, ano, pagina)
            return False

    return True


# ---------------------------------------------------------------------------
# Helpers privados
# ---------------------------------------------------------------------------
def _tem_mais_paginas(dados: dict) -> bool:
    return bool(dados) and dados.get("paginasRestantes", 0) > 0


def _caminho_legado(pasta: str, label: str, sigla: str, ano: int, pagina: int) -> str:
    return f"{pasta}/{label}_{sigla}_{ano}_p{pagina}.json"


def _caminho_14133(pasta: str, sigla: str, modalidade: str, ano: int, pagina: int) -> str:
    return f"{pasta}/pncp_{sigla}_{modalidade}_{ano}_p{pagina}.json"


def _montar_params_legado(unidade: dict, ano: int, endpoint: dict, pagina: int) -> dict:
    params: dict = {
        "pagina": pagina,
        "tamanhoPagina": _TAMANHO_PAGINA,
        endpoint["p_uasg"]: unidade["codigo"],
    }
    if endpoint["label"] == "dispensa":
        params["dt_ano_aviso"] = ano
    else:
        params[f"{endpoint['p_data']}_inicial"] = f"{ano}-01-01"
        params[f"{endpoint['p_data']}_final"] = f"{ano}-12-31"
    return params


def _montar_params_14133(unidade: dict, ano: int, cod_modalidade: int, pagina: int) -> dict:
    return {
        "pagina": pagina,
        "tamanhoPagina": _TAMANHO_PAGINA,
        "unidadeOrgaoCodigoUnidade": unidade["codigo"],
        "dataPublicacaoPncpInicial": f"{ano}-01-01",
        "dataPublicacaoPncpFinal": f"{ano}-12-31",
        "codigoModalidade": cod_modalidade,
    }
