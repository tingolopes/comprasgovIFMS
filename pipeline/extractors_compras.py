"""
pipeline/extractors_compras.py
------------------------------
Funções de extração das compras públicas.

  extrair_legado(unidade, ano, endpoint)  → bool
  extrair_14133(unidade, ano, cod_mod, nome_mod)  → bool

Ambas retornam True em caso de sucesso (ou skip) e False em caso de falha.
Consomem config/config.py e pipeline/api_client.py — sem estado próprio.
"""

import os

from config.config import CONFIG_APIS, PIPELINE_CONFIG
from pipeline.api_client import consultar_api, salvar_dados, verificar_sucesso, deve_reverificar_pncp
from pipeline.logger import log_info, log_skip

# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------


def _log(linha: str, skip: bool = False) -> None:
    if skip:
        log_skip()
    else:
        log_info(linha)


# ---------------------------------------------------------------------------
# Motor Legado
# ---------------------------------------------------------------------------

def extrair_legado(unidade: dict, ano: int, endpoint: dict) -> bool:
    """
    Extrai uma combinação (unidade × ano × endpoint) do módulo Legado.
    Lida com paginação automaticamente.
    """
    cfg = CONFIG_APIS["LEGADO"]
    pasta = cfg["pasta_cache"]
    os.makedirs(pasta, exist_ok=True)

    pagina = 1
    while True:
        arquivo = os.path.join(
            pasta,
            f"{endpoint['label']}_{unidade['sigla']}_{ano}_p{pagina}.json",
        )

        sucesso, dados_cache = verificar_sucesso(arquivo)

        if sucesso:
            respostas = dados_cache.get("respostas", {})
            if not respostas.get("resultado", []):
                break   # página vazia — fim da série
            _log(
                f"⏭️  SKIP | {unidade['sigla']} | "
                f"{endpoint['label'].upper():<15} | {ano}",
                skip=True,
            )
            if respostas.get("paginasRestantes", 0) > 0:
                pagina += 1
                continue
            break

        # --- Monta parâmetros ---
        params: dict = {
            "pagina": pagina,
            "tamanhoPagina": PIPELINE_CONFIG["tamanho_pagina"],
            endpoint["p_uasg"]: unidade["codigo"],
        }
        if endpoint["label"] == "dispensa":
            params["dt_ano_aviso"] = ano
        else:
            params[f"{endpoint['p_data']}_inicial"] = f"{ano}-01-01"
            params[f"{endpoint['p_data']}_final"] = f"{ano}-12-31"

        url = f"{cfg['base_url']}{endpoint['path']}"
        dados, status = consultar_api(url, params)
        salvar_dados(arquivo, url, params, dados, status)

        if status == "SUCESSO":
            _log(
                f"✅ DONE | {unidade['sigla']} | "
                f"{endpoint['label'].upper():<15} | {ano}"
            )
            if dados.get("paginasRestantes", 0) > 0:
                pagina += 1
                continue
            break
        else:
            _log(
                f"❌ FAIL | {unidade['sigla']} | "
                f"{endpoint['label'].upper():<17} | {ano}"
            )
            return False

    return True


# ---------------------------------------------------------------------------
# Motor Lei 14.133 / PNCP
# ---------------------------------------------------------------------------

def extrair_14133(unidade: dict, ano: int, cod_mod: int, nome_mod: str) -> bool:
    """
    Extrai uma combinação (unidade × ano × modalidade) do módulo PNCP/14133.
    Lida com paginação e re-verificação de registros em aberto.
    """
    cfg = CONFIG_APIS["LEI14133"]
    pasta = cfg["pasta_cache"]
    os.makedirs(pasta, exist_ok=True)

    pagina = 1
    while True:
        arquivo = os.path.join(
            pasta,
            f"pncp_{unidade['sigla']}_{nome_mod}_{ano}_p{pagina}.json",
        )

        sucesso, dados_cache = verificar_sucesso(arquivo)

        if sucesso:
            respostas = dados_cache.get("respostas", {})
            tem_resultado = bool(respostas.get("resultado", []))

            # Pula se não há resultado ou se o cache ainda é válido
            if not tem_resultado or not deve_reverificar_pncp(dados_cache):
                _log(
                    f"⏭️  SKIP | {unidade['sigla']} | "
                    f"PNCP-{nome_mod.upper():<12} | {ano}",
                    skip=True,
                )
                if respostas.get("paginasRestantes", 0) > 0 and tem_resultado:
                    pagina += 1
                    continue
                break

        # --- Parâmetros ---
        params: dict = {
            "pagina": pagina,
            "tamanhoPagina": PIPELINE_CONFIG["tamanho_pagina"],
            "unidadeOrgaoCodigoUnidade":   unidade["codigo"],
            "dataPublicacaoPncpInicial":   f"{ano}-01-01",
            "dataPublicacaoPncpFinal":     f"{ano}-12-31",
            "codigoModalidade":            cod_mod,
        }

        url = f"{cfg['base_url']}{cfg['path']}"
        dados, status = consultar_api(url, params)
        salvar_dados(arquivo, url, params, dados, status)

        if status == "SUCESSO":
            _log(
                f"✅ DONE | {unidade['sigla']} | "
                f"PNCP-{nome_mod.upper():<12} | {ano}"
            )
            if dados.get("paginasRestantes", 0) > 0:
                pagina += 1
                continue
            break
        else:
            _log(
                f"❌ FAIL | {unidade['sigla']} | "
                f"PNCP-{nome_mod.upper():<12} | {ano}"
            )
            return False

    return True
