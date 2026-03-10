"""
cache.py
--------
Leitura e escrita de cache em JSON.
Responsabilidade única: persistir e recuperar resultados de chamadas à API.
"""

import json
import os
from datetime import datetime
from typing import Any
from urllib.parse import urlencode

from config import SITUACOES_FINAIS_PNCP, PIPELINE_CONFIG
from logger import log_erro


# ---------------------------------------------------------------------------
# Leitura
# ---------------------------------------------------------------------------
def verificar_sucesso_anterior(caminho: str) -> tuple[bool, dict | None]:
    """
    Retorna (True, dados) se o arquivo existir e tiver status SUCESSO.
    Retorna (False, None) caso contrário.
    """
    if not os.path.exists(caminho):
        return False, None

    try:
        with open(caminho, "r", encoding="utf-8") as f:
            dados = json.load(f)
        status = dados.get("metadata", {}).get("status")
        return (status == "SUCESSO"), dados
    except Exception as exc:
        log_erro("Cache inválido em %s: %s", caminho, exc)
        return False, None


def deve_reverificar_pncp(dados_cache: dict) -> bool:
    """
    Retorna True se o cache PNCP contém contratos ainda em aberto
    e já passou o período de validade configurado.
    """
    dias_validade = PIPELINE_CONFIG["dias_validade_cache_pncp"]
    resultados = dados_cache.get("respostas", {}).get("resultado", [])

    if not resultados:
        return False

    tem_aberto = any(
        c.get("situacaoCompraIdPncp") not in SITUACOES_FINAIS_PNCP
        for c in resultados
    )
    if not tem_aberto:
        return False

    data_str = dados_cache.get("metadata", {}).get("data_extracao", "")
    try:
        data_extracao = datetime.strptime(data_str, "%Y-%m-%d %H:%M:%S")
        return (datetime.now() - data_extracao).days >= dias_validade
    except ValueError:
        return False


# ---------------------------------------------------------------------------
# Escrita
# ---------------------------------------------------------------------------
def salvar_dados(
    caminho: str,
    url_base: str,
    params: dict,
    conteudo: Any,
    status: str = "SUCESSO",
) -> None:
    """
    Persiste a resposta da API com envelope de metadados.

    Regra de ouro: nunca sobrescreve um cache de SUCESSO com uma falha.
    """
    if status != "SUCESSO" and _cache_valido_existe(caminho):
        return  # Preserva o dado bom anterior

    envelope = {
        "metadata": {
            "url_consultada": f"{url_base}?{urlencode(params)}",
            "data_extracao": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": status,
        },
        "respostas": conteudo,
    }

    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(envelope, f, ensure_ascii=False, indent=4)


# ---------------------------------------------------------------------------
# Interno
# ---------------------------------------------------------------------------
def _cache_valido_existe(caminho: str) -> bool:
    """Verifica silenciosamente se há um SUCESSO salvo no caminho."""
    if not os.path.exists(caminho):
        return False
    try:
        with open(caminho, "r", encoding="utf-8") as f:
            dados = json.load(f)
        return dados.get("metadata", {}).get("status") == "SUCESSO"
    except Exception:
        return False
