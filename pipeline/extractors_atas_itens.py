"""
pipeline/extractors_atas_itens.py
-----------------------------------
Extrai itens das Atas de Registro de Preço (ARP) via endpoint 2_consultarARPItem.

Consulta por janelas anuais (01/01 → 31/12) a partir de 2023.
Como a vigência máxima de uma ata é 2 anos (Lei 14.133), o extrator
cobre até ano_atual + 1 para capturar prorrogações em vigor.

Deduplicação por (numeroControlePncpAta + numeroItem) feita no transformer.

Uso como módulo:
    from pipeline.extractors_atas_itens import executar
    falhas = executar()

Uso via CLI:
    python -m pipeline.extractors_atas_itens
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
_BASE_URL = CONFIG_ATAS["base_url"]
_PATH = "/modulo-arp/2_consultarARPItem"
_UASG = CONFIG_ATAS["uasg"]
_PASTA = CONFIG_ATAS["pasta_cache_itens"]
_ANOS = CONFIG_ATAS["anos_itens"]          # 2023 → ano_atual + 1

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
    ok = (
        dados.get("metadata", {}).get("status") == "SUCESSO"
        and extraido_hoje(dados)
    )
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
# Tarefa individual — paginação em loop interno
# ---------------------------------------------------------------------------

def _processar(t: dict) -> str:
    """
    Extrai todos os itens de uma janela anual, percorrendo páginas em loop.
    Arquivo: atas_itens_RT_{ano}_p{pagina}.json
    """
    pagina = 1

    while True:
        nome = os.path.join(
            _PASTA, f"atas_itens_{_UASG['sigla']}_{t['ano']}_p{pagina}.json"
        )
        ok, cache = _verificar_sucesso(nome)

        if ok:
            pag_rest = cache.get("respostas", {}).get("paginasRestantes", 0)
            if pag_rest and pag_rest > 0:
                pagina += 1
                continue
            # Todas as páginas já estão em cache
            return f"⏭️ SKIP | {_UASG['sigla']} | {t['ano']}"

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
            pag_rest = (
                dados.get("respostas", {}).get("paginasRestantes", 0)
                if isinstance(dados, dict) else 0
            )
            if pag_rest and pag_rest > 0:
                pagina += 1
                continue
            return f"✅ DONE | {_UASG['sigla']} | {t['ano']}"
        else:
            return f"❌ {status} | {_UASG['sigla']} | {t['ano']}"


# ---------------------------------------------------------------------------
# Montagem da fila — uma tarefa por ano
# ---------------------------------------------------------------------------

def _montar_fila() -> list[dict]:
    """
    Verifica se todas as páginas do ano estão em cache com SUCESSO.
    Segue a cadeia de paginasRestantes a partir da p1 para saber
    quantas páginas existem — só pula o ano se todas estiverem completas.
    """
    fila = []
    for ano in _ANOS:
        pagina = 1
        ano_completo = True

        while True:
            nome = os.path.join(
                _PASTA, f"atas_itens_{_UASG['sigla']}_{ano}_p{pagina}.json"
            )
            ok, cache = _verificar_sucesso(nome)

            if not ok:
                # Esta página não está em cache — ano precisa ser processado
                ano_completo = False
                break

            pag_rest = cache.get("respostas", {}).get("paginasRestantes", 0)
            if pag_rest and pag_rest > 0:
                pagina += 1  # Verifica a próxima página
            else:
                break  # Última página encontrada e está OK

        if not ano_completo:
            fila.append({"ano": ano})

    return fila


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def executar() -> int:
    """Extrai itens das atas ARP. Retorna número de falhas."""
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
        f"🚀 INICIANDO EXTRAÇÃO DE ITENS DE ATAS | WORKERS: {workers} | TOTAL: {total}\n")

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
                            f"[{ts}] ⏭️  SKIPs: {skips} | "
                            f"{concluidas}/{total} ({perc:.1f}%) | Falhas: {erros}"
                        )
                else:
                    print(
                        f"[{ts}] {res} | "
                        f"{concluidas}/{total} ({perc:.1f}%) | Falhas: {erros}"
                    )
        except KeyboardInterrupt:
            print("\n🛑 Interrompido pelo usuário.")
            pool.shutdown(wait=False, cancel_futures=True)
            sys.exit(0)

    print(f"\n✅ FIM | Falhas: {erros} | SKIPs: {skips}")
    return erros


if __name__ == "__main__":
    sys.exit(0 if executar() == 0 else 1)
