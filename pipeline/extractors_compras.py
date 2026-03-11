"""
pipeline/extractors_compras.py
------------------------------
Funções de extração das compras públicas.

  extrair_legado(unidade, ano, endpoint)         → str  (resultado da tarefa)
  extrair_14133(unidade, ano, cod_mod, nome_mod) → str  (resultado da tarefa)

Retornam uma string descritiva que começa com:
  "✅ DONE"  — nova consulta com sucesso
  "⏭️  SKIP"  — cache válido aproveitado
  "❌ FAIL"  — falha na API
"""

import os

from config.config import CONFIG_APIS, PIPELINE_CONFIG
from pipeline.api_client import consultar_api, salvar_dados, verificar_sucesso, deve_reverificar_pncp


# ---------------------------------------------------------------------------
# Motor Legado
# ---------------------------------------------------------------------------

def extrair_legado(unidade: dict, ano: int, endpoint: dict) -> str:
    cfg = CONFIG_APIS["LEGADO"]
    pasta = cfg["pasta_cache"]
    os.makedirs(pasta, exist_ok=True)

    label = endpoint["label"].upper()
    sigla = unidade["sigla"]

    pagina = 1
    while True:
        arquivo = os.path.join(
            pasta, f"{endpoint['label']}_{sigla}_{ano}_p{pagina}.json")
        sucesso, dados_cache = verificar_sucesso(arquivo)

        if sucesso:
            respostas = dados_cache.get("respostas", {})
            if not respostas.get("resultado", []):
                break   # página vazia — fim da série, conta como skip
            if respostas.get("paginasRestantes", 0) > 0:
                pagina += 1
                continue
            break   # última página em cache — skip

        # --- Monta parâmetros e consulta ---
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
            if dados.get("paginasRestantes", 0) > 0:
                pagina += 1
                continue
            return f"✅ DONE | {sigla} | {label:<17} | {ano}"
        else:
            return f"❌ FAIL | {sigla} | {label:<17} | {ano}"

    return f"⏭️  SKIP | {sigla} | {label:<17} | {ano}"


# ---------------------------------------------------------------------------
# Motor Lei 14.133 / PNCP
# ---------------------------------------------------------------------------

def extrair_14133(unidade: dict, ano: int, cod_mod: int, nome_mod: str) -> str:
    cfg = CONFIG_APIS["LEI14133"]
    pasta = cfg["pasta_cache"]
    os.makedirs(pasta, exist_ok=True)

    sigla = unidade["sigla"]
    mod_label = f"PNCP-{nome_mod.upper()}"

    pagina = 1
    while True:
        arquivo = os.path.join(
            pasta, f"pncp_{sigla}_{nome_mod}_{ano}_p{pagina}.json")
        sucesso, dados_cache = verificar_sucesso(arquivo)

        if sucesso:
            respostas = dados_cache.get("respostas", {})
            tem_result = bool(respostas.get("resultado", []))

            if not tem_result or not deve_reverificar_pncp(dados_cache):
                if respostas.get("paginasRestantes", 0) > 0 and tem_result:
                    pagina += 1
                    continue
                break   # skip

        # --- Parâmetros e consulta ---
        params: dict = {
            "pagina": pagina,
            "tamanhoPagina": PIPELINE_CONFIG["tamanho_pagina"],
            "unidadeOrgaoCodigoUnidade": unidade["codigo"],
            "dataPublicacaoPncpInicial": f"{ano}-01-01",
            "dataPublicacaoPncpFinal":   f"{ano}-12-31",
            "codigoModalidade":          cod_mod,
        }

        url = f"{cfg['base_url']}{cfg['path']}"
        dados, status = consultar_api(url, params)
        salvar_dados(arquivo, url, params, dados, status)

        if status == "SUCESSO":
            if dados.get("paginasRestantes", 0) > 0:
                pagina += 1
                continue
            return f"✅ DONE | {sigla} | {mod_label:<17} | {ano}"
        else:
            return f"❌ FAIL | {sigla} | {mod_label:<17} | {ano}"

    return f"⏭️  SKIP | {sigla} | {mod_label:<17} | {ano}"
