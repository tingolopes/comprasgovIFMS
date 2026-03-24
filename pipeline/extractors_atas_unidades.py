"""
pipeline/extractors_atas_unidades.py
--------------------------------------
Extrai unidades participantes por item de ata via 3_consultarUnidadesItem.

Uma consulta por (numeroAta × numeroItem) — retorna todas as unidades
participantes e não participantes com quantidades reservadas e saldos.
Dado estático: skip permanente por SUCESSO (participantes definidos
na licitação e não mudam após assinatura).

Fila montada a partir do cache temp/atas_itens/ (itens já extraídos).

Nomenclatura dos arquivos:
    atas_unidades_RT_{numero_ata}_{ano}_{numero_item}_p{pagina}.json
    Ex: atas_unidades_RT_00001_2023_00001_p1.json

Uso como módulo:
    from pipeline.extractors_atas_unidades import executar
    falhas = executar()

Uso via CLI:
    python -m pipeline.extractors_atas_unidades
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
_PATH = "/modulo-arp/3_consultarUnidadesItem"
_UASG = CONFIG_ATAS["uasg"]
_PASTA = CONFIG_ATAS["pasta_cache_unidades"]
_PASTA_ITENS = CONFIG_ATAS["pasta_cache_itens"]

_LOG_INTERVALO_SKIP = PIPELINE_CONFIG.get("log_intervalo_skip", 50)


# ---------------------------------------------------------------------------
# Helpers de nome de arquivo
# ---------------------------------------------------------------------------

def _slug(numero_ata: str) -> str:
    """'00001/2023' → '00001_2023'"""
    return numero_ata.replace("/", "_")


def _nome_arquivo(numero_ata: str, numero_item: str, pagina: int) -> str:
    return os.path.join(
        _PASTA,
        f"atas_unidades_{_UASG['sigla']}_{_slug(numero_ata)}_{numero_item}_p{pagina}.json"
    )


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
        ok, _ = _verificar_sucesso(caminho)
        if ok:
            return

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
    """Extrai unidades de um item de ata, percorrendo páginas em loop."""
    numero_ata = t["numero_ata"]
    numero_item = t["numero_item"]
    pagina = 1

    while True:
        nome = _nome_arquivo(numero_ata, numero_item, pagina)
        ok, cache = _verificar_sucesso(nome)

        if ok:
            pag_rest = cache.get("respostas", {}).get("paginasRestantes", 0)
            if pag_rest and pag_rest > 0:
                pagina += 1
                continue
            return f"⏭️ SKIP | {numero_ata} | item {numero_item}"

        url = f"{_BASE_URL}{_PATH}"
        params = {
            "numeroAta":          numero_ata,
            "unidadeGerenciadora": _UASG["codigo"],
            "numeroItem":         numero_item,
            "pagina":             pagina,
            "tamanhoPagina":      PIPELINE_CONFIG["tamanho_pagina"],
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
            return f"✅ DONE | {numero_ata} | item {numero_item}"
        else:
            return f"❌ {status} | {numero_ata} | item {numero_item}"


# ---------------------------------------------------------------------------
# Montagem da fila — lê itens do cache temp/atas_itens/
# ---------------------------------------------------------------------------

def _montar_fila() -> list[dict]:
    """
    Lê todos os JSONs de atas_itens e monta uma tarefa por (ata × item).
    Só entra na fila se a p1 ainda não tem SUCESSO.
    """
    vistos: set[tuple[str, str]] = set()
    fila = []

    for arq in sorted(os.listdir(_PASTA_ITENS)):
        if not arq.endswith(".json"):
            continue
        dados = _carregar_json(os.path.join(_PASTA_ITENS, arq))
        if dados.get("metadata", {}).get("status") != "SUCESSO":
            continue

        resultado = dados.get("respostas", {}).get("resultado", []) or []
        for reg in resultado:
            num_ata = reg.get("numeroAtaRegistroPreco", "")
            num_item = reg.get("numeroItem", "")
            if not num_ata or not num_item:
                continue

            chave = (num_ata, num_item)
            if chave in vistos:
                continue
            vistos.add(chave)

            nome_p1 = _nome_arquivo(num_ata, num_item, 1)
            ok, _ = _verificar_sucesso(nome_p1)
            if not ok:
                fila.append({"numero_ata": num_ata, "numero_item": num_item})

    return fila


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def executar() -> int:
    """Extrai unidades participantes das atas ARP. Retorna número de falhas."""
    os.makedirs(_PASTA, exist_ok=True)

    if not os.path.exists(_PASTA_ITENS) or not os.listdir(_PASTA_ITENS):
        print("⚠️  Cache de itens de atas vazio. Execute extrator_atas_itens primeiro.")
        return 0

    fila = _montar_fila()
    total = len(fila)

    print(f"   UASG        : {_UASG['sigla']} ({_UASG['codigo']})")
    print(f"   A extrair   : {total}\n")

    if total == 0:
        print("⏭️  Tudo em cache. Nada a extrair.")
        return 0

    workers = PIPELINE_CONFIG.get("max_workers_atas", 3)
    print(
        f"🚀 INICIANDO EXTRAÇÃO DE UNIDADES | WORKERS: {workers} | TOTAL: {total}\n")

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
