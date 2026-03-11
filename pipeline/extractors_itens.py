"""
pipeline/extractors_itens.py
-----------------------------
Extrai itens de cada compra já presente em temp/compras/.

Estratégia: consulta TODOS os endpoints de itens para CADA idCompra.
Respostas vazias são ignoradas naturalmente pelo transformer.

Uso como módulo:
    from pipeline.extractors_itens import executar
    falhas = executar()   # retorna int

Uso via CLI:
    python -m pipeline.extractors_itens
"""

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urlencode

import requests

from config.config import CONFIG_APIS, PIPELINE_CONFIG

# ---------------------------------------------------------------------------
# Configuração local
# ---------------------------------------------------------------------------
_PASTA_COMPRAS = CONFIG_APIS["LEGADO"]["pasta_cache"]   # temp/compras (única)
_PASTA_ITENS = "temp/itens"
_BASE_URL = CONFIG_APIS["LEGADO"]["base_url"]

ENDPOINTS: dict[str, dict] = {
    "E2": {
        "path":      "/modulo-legado/2.1_consultarItemLicitacao_Id",
        "params_fn": lambda id_c: {"id_compra": id_c},
        "paginavel": False,
    },
    "E4": {
        "path":      "/modulo-legado/4.1_consultarItensPregoes_Id",
        "params_fn": lambda id_c: {"id_compra": id_c},
        "paginavel": False,
    },
    "E6": {
        "path":      "/modulo-legado/6.1_consultarItensComprasSemLicitacao_Id",
        "params_fn": lambda id_c: {"id_compra": id_c},
        "paginavel": False,
    },
    "pncp": {
        "path":      "/modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id",
        "params_fn": lambda id_c: {"tipo": "idCompra", "codigo": id_c},
        "paginavel": True,
    },
}

_TODOS_SUFIXOS = list(ENDPOINTS.keys())   # ["E2", "E4", "E6", "pncp"]
_LOG_INTERVALO_SKIP = PIPELINE_CONFIG.get("log_intervalo_skip", 100)


# ---------------------------------------------------------------------------
# Utilitários de cache
# ---------------------------------------------------------------------------

def _carregar_json(caminho: str) -> dict:
    if not os.path.exists(caminho):
        return {}
    try:
        with open(caminho, "r", encoding="utf-8") as f:
            dados = json.load(f)
        return dados if isinstance(dados, dict) else {}
    except Exception as exc:
        print(f"⚠️  Erro ao ler {caminho}: {exc}")
        return {}


def _verificar_sucesso(caminho: str) -> tuple[bool, dict]:
    dados = _carregar_json(caminho)
    ok = dados.get("metadata", {}).get("status") == "SUCESSO"
    return ok, dados


def _salvar(caminho: str, url: str, params: dict,
            conteudo, status: str = "SUCESSO") -> None:
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


def _paginacao(dados: dict) -> tuple[int, bool]:
    """Lê paginasRestantes e resultado de resposta direta ou de envelope."""
    if not isinstance(dados, dict):
        return 0, False

    pag = dados.get("paginasRestantes")
    res = dados.get("resultado")

    if pag is None or res is None:
        r = dados.get("respostas", {})
        if isinstance(r, dict):
            if pag is None:
                pag = r.get("paginasRestantes")
            if res is None:
                res = r.get("resultado")

    return int(pag or 0), bool(res)


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def _get(url: str, params: dict) -> tuple[dict | None, str]:
    for tentativa in range(3):
        try:
            resp = requests.get(
                url, params=params,
                timeout=PIPELINE_CONFIG["timeout_segundos"],
            )
            if resp.status_code == 200:
                return resp.json(), "SUCESSO"
            elif resp.status_code == 429:
                time.sleep(15 * (tentativa + 1))
                continue
            else:
                return None, f"ERRO_{resp.status_code}"
        except Exception as exc:
            print(f"⚠️  Tentativa {tentativa + 1} ({url}): {exc}")
        time.sleep(2)
    return None, "FALHA"


# ---------------------------------------------------------------------------
# Tarefa individual
# ---------------------------------------------------------------------------

def _processar(t: dict) -> str:
    """Processa uma tarefa {id, sufixo}. Retorna string de log."""
    ep = ENDPOINTS[t["sufixo"]]
    pagina = 1

    while True:
        nome = os.path.join(
            _PASTA_ITENS,
            f"itens_{t['id']}_{t['sufixo']}_p{pagina}.json",
        )
        ok, cache = _verificar_sucesso(nome)

        if ok:
            pag_rest, tem_res = _paginacao(cache)
            if not ep["paginavel"] or pag_rest == 0 or not tem_res:
                return f"⏭️ SKIP | {t['id']} | {t['sufixo']}"
            pagina += 1
            continue

        url = f"{_BASE_URL}{ep['path']}"
        params = ep["params_fn"](t["id"])
        if ep["paginavel"]:
            params.update({"pagina": pagina,
                           "tamanhoPagina": PIPELINE_CONFIG["tamanho_pagina"]})

        dados, status = _get(url, params)
        _salvar(nome, url, params, dados, status)

        if status == "SUCESSO" and ep["paginavel"]:
            pag_rest, tem_res = _paginacao(dados)
            if pag_rest > 0 and tem_res:
                pagina += 1
                continue

        icone = "✅" if status == "SUCESSO" else "❌"
        return f"{icone} {status} | {t['id']} | {t['sufixo']}"


# ---------------------------------------------------------------------------
# Montagem da fila
# ---------------------------------------------------------------------------

def _ids_unicos_de_pasta(pasta: str) -> list[str]:
    """
    Coleta todos os idCompra distintos dos arquivos de compras com SUCESSO.
    Não usa a URL — simplesmente pega todos os IDs.
    """
    ids: set[str] = set()
    if not os.path.exists(pasta):
        return []

    for arq in sorted(os.listdir(pasta)):
        if not arq.endswith(".json"):
            continue
        dados = _carregar_json(os.path.join(pasta, arq))
        if dados.get("metadata", {}).get("status") != "SUCESSO":
            continue
        respostas = dados.get("respostas", {})
        if not isinstance(respostas, dict):
            continue
        for compra in respostas.get("resultado", []):
            id_c = compra.get("idCompra") or compra.get("id_compra")
            if id_c:
                ids.add(str(id_c))

    return sorted(ids)


def _montar_fila() -> list[dict]:
    """
    Monta uma tarefa para CADA combinação (idCompra × endpoint).
    Todos os 4 endpoints (E2, E4, E6, pncp) são consultados para
    cada compra — respostas vazias são ignoradas pelo transformer.
    O cache garante que consultas anteriores não sejam repetidas.
    """
    print("🔍 Coletando ids de compras para montar a fila de itens...\n")

    ids = _ids_unicos_de_pasta(_PASTA_COMPRAS)

    if not ids:
        print("  ⚠️  Nenhum idCompra encontrado em", _PASTA_COMPRAS)
        return []

    # Gera (id × sufixo) pulando pares que já têm cache SUCESSO na p1
    fila: list[dict] = []
    ja_ok = 0
    for id_c in ids:
        for sufixo in _TODOS_SUFIXOS:
            nome_p1 = os.path.join(
                _PASTA_ITENS, f"itens_{id_c}_{sufixo}_p1.json")
            ok, _ = _verificar_sucesso(nome_p1)
            if ok:
                ja_ok += 1
            else:
                fila.append({"id": id_c, "sufixo": sufixo})

    n_endpoints = len(_TODOS_SUFIXOS)
    print(f"   IDs únicos  : {len(ids)}")
    print(f"   Endpoints   : {n_endpoints} ({', '.join(_TODOS_SUFIXOS)})")
    print(f"   Já em cache : {ja_ok}")
    print(f"   A extrair   : {len(fila)}")
    print(f"   TOTAL fila  : {len(fila)}\n")

    return fila


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def executar() -> int:
    """
    Executa a extração completa de itens.
    Retorna o número de falhas (0 = tudo OK).
    """
    os.makedirs(_PASTA_ITENS, exist_ok=True)

    fila = _montar_fila()
    total = len(fila)

    if total == 0:
        print("⚠️  Nenhuma compra encontrada ou todas já em cache.")
        return 0

    workers = PIPELINE_CONFIG["max_workers_itens"]
    print(
        f"🚀 INICIANDO EXTRAÇÃO DE ITENS | WORKERS: {workers} | TOTAL: {total}\n")

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
                            f"[{ts}] ⏭️  SKIPs: {skips} "
                            f"| {concluidas}/{total} ({perc:.1f}%) "
                            f"| Falhas: {erros}"
                        )
                else:
                    print(
                        f"[{ts}] {res} "
                        f"| {concluidas}/{total} ({perc:.1f}%) "
                        f"| Falhas: {erros}"
                    )
        except KeyboardInterrupt:
            print("\n🛑 Interrompido pelo usuário.")
            pool.shutdown(wait=False, cancel_futures=True)
            sys.exit(0)

    print(f"\n✅ FIM | Falhas: {erros} | SKIPs: {skips}")
    return erros


if __name__ == "__main__":
    sys.exit(0 if executar() == 0 else 1)
