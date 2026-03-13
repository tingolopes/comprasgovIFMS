"""
pipeline/extractors_atas_saldos.py
------------------------------------
Extrai saldos de empenho por ata via endpoint 4_consultarEmpenhosSaldoItem.

Uma consulta por ata — retorna todos os itens × unidades com quantidades
empenhadas e saldo restante. Dado dinâmico: re-verifica a cada
DIAS_VALIDADE_CACHE_SALDOS dias pois saldo muda com novos empenhos.

Fila montada a partir do cache temp/atas/ (atas já extraídas).

Nomenclatura dos arquivos:
    atas_saldos_RT_{numero_ata}_{ano}_p{pagina}.json
    Ex: atas_saldos_RT_00001_2023_p1.json

Uso como módulo:
    from pipeline.extractors_atas_saldos import executar
    falhas = executar()

Uso via CLI:
    python -m pipeline.extractors_atas_saldos
"""

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urlencode

import requests

from config.config import CONFIG_ATAS, HTTP_HEADERS, PIPELINE_CONFIG

# ---------------------------------------------------------------------------
# Configuração local
# ---------------------------------------------------------------------------
_BASE_URL = CONFIG_ATAS["base_url"]
_PATH = "/modulo-arp/4_consultarEmpenhosSaldoItem"
_UASG = CONFIG_ATAS["uasg"]
_PASTA = CONFIG_ATAS["pasta_cache_saldos"]
_PASTA_ATAS = CONFIG_ATAS["pasta_cache"]

_DIAS_VALIDADE = PIPELINE_CONFIG.get("dias_validade_cache_saldos", 3)
_LOG_INTERVALO_SKIP = PIPELINE_CONFIG.get("log_intervalo_skip", 50)


# ---------------------------------------------------------------------------
# Helpers de nome de arquivo
# ---------------------------------------------------------------------------

def _slug(numero_ata: str) -> str:
    """'00001/2023' → '00001_2023'"""
    return numero_ata.replace("/", "_")


def _nome_arquivo(numero_ata: str, pagina: int) -> str:
    return os.path.join(
        _PASTA, f"atas_saldos_{_UASG['sigla']}_{_slug(numero_ata)}_p{pagina}.json"
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


def _verificar_cache(caminho: str) -> tuple[bool, dict]:
    """
    Retorna (valido, dados).
    Inválido se: não existe, não é SUCESSO, ou cache mais velho que DIAS_VALIDADE.
    """
    dados = _carregar_json(caminho)
    if dados.get("metadata", {}).get("status") != "SUCESSO":
        return False, dados

    data_ext_str = dados.get("metadata", {}).get("data_extracao", "")
    try:
        data_ext = datetime.strptime(data_ext_str, "%Y-%m-%d %H:%M:%S")
        if (datetime.now() - data_ext).days >= _DIAS_VALIDADE:
            return False, dados   # Cache expirado — re-consulta
    except ValueError:
        return False, dados

    return True, dados


def _e_sucesso(caminho: str) -> bool:
    """Retorna True se o arquivo existe e tem status SUCESSO — ignora validade."""
    dados = _carregar_json(caminho)
    return dados.get("metadata", {}).get("status") == "SUCESSO"


def _salvar(caminho: str, url: str, params: dict,
            conteudo, status: str = "SUCESSO") -> None:
    """Nunca sobrescreve cache SUCESSO com falha — independente da validade."""
    if status != "SUCESSO" and os.path.exists(caminho):
        if _e_sucesso(caminho):
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
    timeout = PIPELINE_CONFIG.get(
        "timeout_segundos_saldos",    PIPELINE_CONFIG["timeout_segundos"])
    tentativas = PIPELINE_CONFIG.get(
        "backoff_tentativas_saldos",  PIPELINE_CONFIG["backoff_tentativas"])
    for tentativa in range(1, tentativas + 1):
        try:
            resp = requests.get(url, params=params,
                                headers=HTTP_HEADERS,
                                timeout=timeout)
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
    """Extrai saldo de uma ata, percorrendo páginas em loop."""
    numero_ata = t["numero_ata"]
    pagina = 1

    while True:
        nome = _nome_arquivo(numero_ata, pagina)
        valido, cache = _verificar_cache(nome)

        if valido:
            pag_rest = cache.get("respostas", {}).get("paginasRestantes", 0)
            if pag_rest and pag_rest > 0:
                pagina += 1
                continue
            return f"⏭️ SKIP | {_UASG['sigla']} | {numero_ata}"

        url = f"{_BASE_URL}{_PATH}"
        params = {
            "numeroAta":          numero_ata,
            "unidadeGerenciadora": _UASG["codigo"],
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
            return f"✅ DONE | {_UASG['sigla']} | {numero_ata}"
        else:
            return f"❌ {status} | {_UASG['sigla']} | {numero_ata}"


# ---------------------------------------------------------------------------
# Montagem da fila — lê atas do cache temp/atas/
# ---------------------------------------------------------------------------

def _montar_fila() -> list[dict]:
    """
    Lê todas as atas em cache e monta uma tarefa por ata única.
    Inclui atas cujo saldo ainda não foi consultado OU cujo cache expirou.
    """
    atas: dict[str, bool] = {}  # numero_ata → já visto

    for arq in sorted(os.listdir(_PASTA_ATAS)):
        if not arq.endswith(".json"):
            continue
        dados = _carregar_json(os.path.join(_PASTA_ATAS, arq))
        if dados.get("metadata", {}).get("status") != "SUCESSO":
            continue
        for ata in dados.get("respostas", {}).get("resultado", []):
            num = ata.get("numeroAtaRegistroPreco", "")
            if num and num not in atas:
                atas[num] = True

    fila = []
    for numero_ata in sorted(atas):
        nome_p1 = _nome_arquivo(numero_ata, 1)
        valido, _ = _verificar_cache(nome_p1)
        if not valido:
            fila.append({"numero_ata": numero_ata})

    return fila


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def executar() -> int:
    """Extrai saldos das atas ARP. Retorna número de falhas."""
    os.makedirs(_PASTA, exist_ok=True)

    if not os.path.exists(_PASTA_ATAS) or not os.listdir(_PASTA_ATAS):
        print("⚠️  Cache de atas vazio. Execute extrator_atas primeiro.")
        return 0

    fila = _montar_fila()
    total = len(fila)

    print(f"   UASG             : {_UASG['sigla']} ({_UASG['codigo']})")
    print(f"   Validade cache   : {_DIAS_VALIDADE} dias")
    print(f"   A extrair/atualizar: {total}\n")

    if total == 0:
        print("⏭️  Tudo em cache e dentro da validade. Nada a extrair.")
        return 0

    workers = PIPELINE_CONFIG.get("max_workers_atas", 3)
    print(
        f"🚀 INICIANDO EXTRAÇÃO DE SALDOS | WORKERS: {workers} | TOTAL: {total}\n")

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
