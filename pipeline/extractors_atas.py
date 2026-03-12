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

# ---------------------------------------------------------------------------
# Configuração local
# ---------------------------------------------------------------------------
_CFG = CONFIG_ATAS
_PASTA = _CFG["pasta_cache"]
_BASE_URL = _CFG["base_url"]
_PATH = _CFG["path"]
_UASG = _CFG["uasg"]
_ANOS = _CFG["anos"]

_LOG_INTERVALO_SKIP = PIPELINE_CONFIG.get("log_intervalo_skip", 50)


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

def _carregar_json(caminho: str) -> dict:
    if not os.path.exists(caminho):
        return {}
    try:
        with open(caminho, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception as exc:
        print(f"⚠️  Erro ao ler {caminho}: {exc}")
        return {}


def _verificar_sucesso(caminho: str) -> tuple[bool, dict]:
    dados = _carregar_json(caminho)
    ok = dados.get("metadata", {}).get("status") == "SUCESSO"
    return ok, dados


def _salvar(caminho: str, url: str, params: dict,
            conteudo, status: str = "SUCESSO") -> None:
    """Nunca sobrescreve cache SUCESSO com falha."""
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
        ok, cache = _verificar_sucesso(nome)

        if ok:
            pag_rest = cache.get("respostas", {}).get("paginasRestantes", 0)
            if pag_rest and pag_rest > 0:
                pagina += 1
                continue
            return f"⏭️ SKIP | {t['sigla']} | {t['ano']}"

        url = f"{_BASE_URL}{_PATH}"
        params = {
            "pagina":                   pagina,
            "tamanhoPagina":            PIPELINE_CONFIG["tamanho_pagina"],
            "codigoUnidadeGerenciadora": _UASG["codigo"],
            "dataVigenciaInicialMin":   f"{t['ano']}-01-01",
            "dataVigenciaInicialMax":   f"{t['ano']}-12-31",
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
    fila = []
    sigla = _UASG["sigla"]
    for ano in _ANOS:
        # Só entra na fila se a p1 ainda não tem SUCESSO
        nome_p1 = os.path.join(_PASTA, f"atas_{sigla}_{ano}_p1.json")
        ok, _ = _verificar_sucesso(nome_p1)
        if not ok:
            fila.append({"sigla": sigla, "ano": ano, "pagina": 1})
    return fila


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def executar() -> int:
    """Extrai atas ARP. Retorna número de falhas."""
    os.makedirs(_PASTA, exist_ok=True)

    fila = _montar_fila()
    total = len(fila)
    ja_ok = len(_ANOS) - total

    print(f"   UASG        : {_UASG['sigla']} ({_UASG['codigo']})")
    print(f"   Anos        : {_ANOS[0]}–{_ANOS[-1]}")
    print(f"   Já em cache : {ja_ok}")
    print(f"   A extrair   : {total}\n")

    if total == 0:
        print("⏭️  Tudo em cache. Nada a extrair.")
        return 0

    workers = PIPELINE_CONFIG.get("max_workers_atas", 3)
    print(
        f"🚀 INICIANDO EXTRAÇÃO DE ATAS | WORKERS: {workers} | TOTAL: {total}\n")

    concluidas = erros = skips = ultimo_log_skip = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        try:
            futures = {pool.submit(_processar, t): t for t in fila}
            for future in as_completed(futures):
                concluidas += 1
                res = future.result()

                if "❌" in res or "FALHA" in res:
                    erros += 1
                elif "⏭️ SKIP" in res:
                    skips += 1

                perc = concluidas / total * 100
                ts = datetime.now().strftime("%H:%M:%S")

                if "⏭️ SKIP" in res:
                    if (skips - ultimo_log_skip) >= _LOG_INTERVALO_SKIP:
                        ultimo_log_skip = skips
                        print(
                            f"[{ts}] ⏭️  SKIPs: {skips} | {concluidas}/{total} ({perc:.1f}%) | Falhas: {erros}")
                else:
                    print(
                        f"[{ts}] {res} | {concluidas}/{total} ({perc:.1f}%) | Falhas: {erros}")
        except KeyboardInterrupt:
            print("\n🛑 Interrompido pelo usuário.")
            pool.shutdown(wait=False, cancel_futures=True)
            sys.exit(0)

    print(f"\n✅ FIM | Falhas: {erros} | SKIPs: {skips}")
    return erros


if __name__ == "__main__":
    sys.exit(0 if executar() == 0 else 1)
