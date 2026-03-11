"""
pipeline/api_client.py
----------------------
Utilitários compartilhados de acesso à API e cache em disco.
"""

import json
import os
import time
from datetime import datetime
from urllib.parse import urlencode

import requests

from config.config import PIPELINE_CONFIG, SITUACOES_FINAIS_PNCP


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

def verificar_sucesso(caminho: str) -> tuple[bool, dict]:
    """Lê o arquivo de cache e retorna (sucesso, dados)."""
    if not os.path.exists(caminho):
        return False, {}
    try:
        with open(caminho, "r", encoding="utf-8") as f:
            dados = json.load(f)
        status = dados.get("metadata", {}).get("status") == "SUCESSO"
        return status, dados
    except Exception as exc:
        print(f"⚠️  Cache inválido em {caminho}: {exc}")
        return False, {}


def salvar_dados(caminho: str, url: str, params: dict,
                 conteudo, status: str = "SUCESSO") -> None:
    """
    Persiste o envelope JSON em disco.
    Trava de segurança: nunca sobrescreve cache SUCESSO com falha.
    """
    if status != "SUCESSO" and os.path.exists(caminho):
        try:
            with open(caminho, "r", encoding="utf-8") as f:
                antigo = json.load(f)
            if antigo.get("metadata", {}).get("status") == "SUCESSO":
                return
        except Exception:
            pass

    envelope = {
        "metadata": {
            "url_consultada": f"{url}?{urlencode(params)}",
            "data_extracao":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status":         status,
        },
        "respostas": conteudo,
    }
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(envelope, f, ensure_ascii=False, indent=4)


def deve_reverificar_pncp(dados_cache: dict) -> bool:
    """
    Retorna True se o cache PNCP precisa ser atualizado:
    - contém registros com situação não-final E
    - a extração tem mais de `dias_validade_cache_pncp` dias.
    """
    respostas = dados_cache.get("respostas", {})
    resultados = respostas.get("resultado", [])
    if not resultados:
        return False

    eh_volatil = any(
        r.get("situacaoCompraIdPncp") not in SITUACOES_FINAIS_PNCP
        for r in resultados
    )
    if not eh_volatil:
        return False

    data_str = dados_cache.get("metadata", {}).get("data_extracao", "")
    try:
        data_ext = datetime.strptime(data_str, "%Y-%m-%d %H:%M:%S")
        dias = PIPELINE_CONFIG["dias_validade_cache_pncp"]
        return (datetime.now() - data_ext).days >= dias
    except ValueError:
        return False


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def consultar_api(url: str, params: dict) -> tuple[dict | None, str]:
    """
    GET com backoff exponencial.
    Retorna (dados, status) onde status é 'SUCESSO' ou 'FALHA'.
    """
    atraso = PIPELINE_CONFIG["backoff_inicial"]
    tentativas = PIPELINE_CONFIG["backoff_tentativas"]

    for tentativa in range(1, tentativas + 1):
        try:
            resp = requests.get(
                url, params=params,
                timeout=PIPELINE_CONFIG["timeout_segundos"],
            )
            if resp.status_code == 200:
                dados = resp.json()
                if isinstance(dados, dict) and "resultado" in dados:
                    return dados, "SUCESSO"
            elif resp.status_code == 429:
                time.sleep(15 * tentativa)
                continue
        except Exception as exc:
            print(f"⚠️  Tentativa {tentativa} falhou ({url}): {exc}")

        time.sleep(atraso)
        atraso *= 2

    return None, "FALHA"
