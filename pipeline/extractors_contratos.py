"""
pipeline/extractors_contratos.py
----------------------------------
Extrai a lista de contratos por UASG via contratos.comprasnet.gov.br.

Uma consulta por UASG — retorna todos os contratos da unidade.
API retorna lista direta, sem paginação.
Cache com validade de DIAS_VALIDADE_CACHE_CONTRATOS dias (dado muda
com aditivos e rescisões frequentes).

Nomenclatura dos arquivos:
    contratos_{sigla}.json
    Ex: contratos_RT.json

Uso como módulo:
    from pipeline.extractors_contratos import executar
    falhas = executar()

Uso via CLI:
    python -m pipeline.extractors_contratos
"""

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urlencode

import requests

from config.config import CONFIG_CONTRATOS, HTTP_HEADERS, PIPELINE_CONFIG

# ---------------------------------------------------------------------------
# Configuração local
# ---------------------------------------------------------------------------
_BASE_URL  = CONFIG_CONTRATOS["base_url"]
_PASTA     = CONFIG_CONTRATOS["pasta_cache"]
_UASGS     = CONFIG_CONTRATOS["uasgs"]

_DIAS_VALIDADE      = PIPELINE_CONFIG.get("dias_validade_cache_contratos", 1)
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


def _e_sucesso(caminho: str) -> bool:
    """Retorna True se o arquivo existe e tem status SUCESSO — ignora validade."""
    return _carregar_json(caminho).get("metadata", {}).get("status") == "SUCESSO"


def _verificar_cache(caminho: str) -> tuple[bool, dict]:
    """
    Retorna (valido, dados).
    Inválido se: não existe, não é SUCESSO, ou mais velho que DIAS_VALIDADE.
    """
    dados = _carregar_json(caminho)
    if dados.get("metadata", {}).get("status") != "SUCESSO":
        return False, dados

    data_str = dados.get("metadata", {}).get("data_extracao", "")
    try:
        data_ext = datetime.strptime(data_str, "%Y-%m-%d %H:%M:%S")
        if (datetime.now() - data_ext).days >= _DIAS_VALIDADE:
            return False, dados
    except ValueError:
        return False, dados

    return True, dados


def _salvar(caminho: str, url: str, conteudo, status: str = "SUCESSO") -> None:
    """Nunca sobrescreve cache SUCESSO com falha."""
    if status != "SUCESSO" and _e_sucesso(caminho):
        return

    envelope = {
        "metadata": {
            "url_consultada": url,
            "data_extracao":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status":         status,
        },
        "respostas": {"resultado": conteudo if conteudo is not None else []},
    }
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(envelope, f, ensure_ascii=False, indent=4)


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def _get(url: str) -> tuple[list | None, str]:
    atraso     = PIPELINE_CONFIG["backoff_inicial"]
    tentativas = PIPELINE_CONFIG["backoff_tentativas"]
    timeout    = PIPELINE_CONFIG["timeout_segundos"]

    for tentativa in range(1, tentativas + 1):
        try:
            resp = requests.get(url, headers=HTTP_HEADERS, timeout=timeout)
            if resp.status_code == 200:
                dados = resp.json()
                # API pode retornar lista direta, {data: []}, ou {_embedded: {}}
                if isinstance(dados, list):
                    return dados, "SUCESSO"
                elif "data" in dados:
                    return dados["data"], "SUCESSO"
                elif "_embedded" in dados:
                    return dados["_embedded"].get("contratos", []), "SUCESSO"
                else:
                    return [dados], "SUCESSO"
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

def _processar(uasg: dict) -> str:
    sigla  = uasg["sigla"]
    codigo = uasg["codigo"]
    nome   = os.path.join(_PASTA, f"contratos_{sigla}.json")

    valido, _ = _verificar_cache(nome)
    if valido:
        return f"⏭️ SKIP | {sigla}"

    url = f"{_BASE_URL}/contrato/ug/{codigo}"
    dados, status = _get(url)

    if status == "SUCESSO" and dados is not None:
        # Enriquece cada contrato com a sigla de origem
        for c in dados:
            c["origem_sigla"] = sigla
            c["origem_uasg"]  = codigo

    _salvar(nome, url, dados, status)

    if status == "SUCESSO":
        return f"✅ DONE | {sigla} | {len(dados or [])} contratos"
    else:
        return f"❌ {status} | {sigla}"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def executar() -> int:
    """Extrai contratos de todas as UASGs. Retorna número de falhas."""
    os.makedirs(_PASTA, exist_ok=True)

    total   = len(_UASGS)
    workers = PIPELINE_CONFIG.get("max_workers_contratos", 5)

    print(f"   UASGs       : {total}")
    print(f"   Validade    : {_DIAS_VALIDADE} dia(s)")
    print(f"\n🚀 INICIANDO EXTRAÇÃO DE CONTRATOS | WORKERS: {workers} | TOTAL: {total}\n")

    concluidas = erros = skips = ultimo_log_skip = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        try:
            futures = {pool.submit(_processar, u): u for u in _UASGS}
            for future in as_completed(futures):
                concluidas += 1
                res = future.result()

                if "❌" in res:
                    erros += 1
                elif "⏭️" in res:
                    skips += 1

                perc = concluidas / total * 100
                ts   = datetime.now().strftime("%H:%M:%S")

                if "⏭️" in res:
                    if (skips - ultimo_log_skip) >= _LOG_INTERVALO_SKIP:
                        ultimo_log_skip = skips
                        print(f"[{ts}] ⏭️  SKIPs: {skips} | {concluidas}/{total} ({perc:.1f}%) | Falhas: {erros}")
                else:
                    print(f"[{ts}] {res} | {concluidas}/{total} ({perc:.1f}%) | Falhas: {erros}")

        except KeyboardInterrupt:
            print("\n🛑 Interrompido pelo usuário.")
            pool.shutdown(wait=False, cancel_futures=True)
            sys.exit(0)

    print(f"\n✅ FIM | Falhas: {erros} | SKIPs: {skips}")
    return erros


if __name__ == "__main__":
    sys.exit(0 if executar() == 0 else 1)
