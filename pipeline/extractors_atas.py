"""
pipeline/extractors_atas.py
----------------------------
Extrai Atas de Registro de Preço (ARP) da unidade gerenciadora (RT/158132).

Consulta por janelas de vigência anuais e salva JSONs em temp/atas/.
A deduplicação por numeroControlePncpAta é feita pelo transformer.

Uso como módulo:
    from pipeline.extractors_atas import executar
    falhas = executar()   # retorna int

Uso via CLI:
    python -m pipeline.extractors_atas
"""

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urlencode

import requests

from config.config import CONFIG_ATAS, PIPELINE_CONFIG
from pipeline.api_client import extraido_hoje

# ---------------------------------------------------------------------------
# Configuração local
# ---------------------------------------------------------------------------
_CFG = CONFIG_ATAS
_PASTA = _CFG["pasta_cache"]
_BASE_URL = _CFG["base_url"]
_PATH = _CFG["path"]
_UASG = _CFG["uasg"]
_ANOS = _CFG["anos"]


# ---------------------------------------------------------------------------
# Salvar
# ---------------------------------------------------------------------------

def _salvar(caminho: str, url: str, params: dict,
            conteudo, status: str = "SUCESSO") -> None:
    envelope = {
        "metadata": {
            "url_consultada": f"{url}?{urlencode(params)}",
            "data_extracao":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status":         status,
        },
        "respostas": conteudo if conteudo is not None else {},
    }
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(envelope, f, ensure_ascii=False, indent=4)


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def _get(url: str, params: dict) -> tuple[dict | None, str]:
    atraso = PIPELINE_CONFIG["backoff_inicial"]
    for tentativa in range(1, PIPELINE_CONFIG["backoff_tentativas"] + 1):
        try:
            resp = requests.get(url, params=params,
                                timeout=PIPELINE_CONFIG["timeout_segundos"])
            if resp.status_code == 200:
                return resp.json(), "SUCESSO"
            elif resp.status_code == 429:
                time.sleep(15 * tentativa)
                continue
            else:
                return None, f"ERRO_{resp.status_code}"
        except Exception as exc:
            print(f"⚠️  Tentativa {tentativa} ({url}): {exc}")
        time.sleep(atraso)
        atraso *= 2
    return None, "FALHA"


# ---------------------------------------------------------------------------
# Tarefa individual
# ---------------------------------------------------------------------------

def _processar(t: dict) -> str:
    """Processa uma tarefa {ano, pagina}. Retorna string de log."""
    pagina = t["pagina"]

    while True:
        nome = os.path.join(
            _PASTA, f"atas_{t['sigla']}_{t['ano']}_p{pagina}.json")
        if os.path.exists(nome):
            try:
                with open(nome, "r", encoding="utf-8") as f:
                    cache = json.load(f) or {}
                if (
                    cache.get("metadata", {}).get("status") == "SUCESSO"
                    and extraido_hoje(cache)
                ):
                    pag_rest = cache.get("respostas", {}).get("paginasRestantes", 0)
                    if pag_rest and pag_rest > 0:
                        pagina += 1
                        continue
                    return f"⏭️ SKIP | {t['sigla']} | {t['ano']}"
            except Exception:
                pass

        url = f"{_BASE_URL}{_PATH}"
        params = {
            "pagina":                    pagina,
            "tamanhoPagina":             PIPELINE_CONFIG["tamanho_pagina"],
            "codigoUnidadeGerenciadora": _UASG["codigo"],
            "dataVigenciaInicialMin":    f"{t['ano']}-01-01",
            "dataVigenciaInicialMax":    f"{t['ano']}-12-31",
        }

        dados, status = _get(url, params)
        _salvar(nome, url, params, dados, status)

        if status == "SUCESSO":
            pag_rest = dados.get("respostas", {}).get(
                "paginasRestantes", 0) if isinstance(dados, dict) else 0
            if pag_rest and pag_rest > 0:
                pagina += 1
                continue
            return f"✅ DONE | {t['sigla']} | {t['ano']}"
        else:
            return f"❌ {status} | {t['sigla']} | {t['ano']}"


# ---------------------------------------------------------------------------
# Montagem da fila
# ---------------------------------------------------------------------------

def _montar_fila() -> list[dict]:
    """Uma tarefa por ano — paginação tratada dentro de _processar."""
    sigla = _UASG["sigla"]
    return [{"sigla": sigla, "ano": ano, "pagina": 1} for ano in _ANOS]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def executar() -> int:
    """Extrai atas ARP. Retorna número de falhas."""
    os.makedirs(_PASTA, exist_ok=True)

    fila = _montar_fila()
    total = len(fila)

    print(f"   UASG      : {_UASG['sigla']} ({_UASG['codigo']})")
    print(f"   Anos      : {_ANOS[0]}–{_ANOS[-1]}")
    print(f"   A extrair : {total}\n")

    workers = PIPELINE_CONFIG.get("max_workers_atas", 3)
    print(f"🚀 INICIANDO EXTRAÇÃO DE ATAS | WORKERS: {workers} | TOTAL: {total}\n")

    concluidas = erros = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        try:
            futures = {pool.submit(_processar, t): t for t in fila}
            for future in as_completed(futures):
                concluidas += 1
                res = future.result()

                if "❌" in res or "FALHA" in res:
                    erros += 1

                perc = concluidas / total * 100
                ts = datetime.now().strftime("%H:%M:%S")
                print(f"[{ts}] {res} | {concluidas}/{total} ({perc:.1f}%) | Falhas: {erros}")

        except KeyboardInterrupt:
            print("\n🛑 Interrompido pelo usuário.")
            pool.shutdown(wait=False, cancel_futures=True)
            sys.exit(0)

    print(f"\n✅ FIM | Falhas: {erros}")
    return erros


if __name__ == "__main__":
    sys.exit(0 if executar() == 0 else 1)
