"""
pipeline/extractors_contratos_responsaveis.py
-----------------------------------------------
Extrai responsáveis de cada contrato via contratos.comprasnet.gov.br.

Fila montada a partir do cache temp/contratos/ — uma tarefa por contrato.
API retorna lista direta, sem paginação.
Cache com validade de DIAS_VALIDADE_CACHE_RESPONSAVEIS dias (fiscal
pode ser substituído a qualquer momento).
Skip permanente para contratos encerrados (vigencia_fim no passado)
que já tenham cache SUCESSO — responsáveis não mudam após encerramento.

Nomenclatura dos arquivos:
    contratos_responsaveis_{id_contrato}.json
    Ex: contratos_responsaveis_12345678.json

Uso como módulo:
    from pipeline.extractors_contratos_responsaveis import executar
    falhas = executar()

Uso via CLI:
    python -m pipeline.extractors_contratos_responsaveis
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
_BASE_URL        = CONFIG_CONTRATOS["base_url"]
_PASTA           = CONFIG_CONTRATOS["pasta_cache_responsaveis"]
_PASTA_CONTRATOS = CONFIG_CONTRATOS["pasta_cache"]

_DIAS_VALIDADE      = PIPELINE_CONFIG.get("dias_validade_cache_responsaveis", 1)
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


def _verificar_cache(caminho: str, encerrado: bool = False) -> tuple[bool, dict]:
    """
    Retorna (valido, dados).
    Contratos encerrados com SUCESSO: skip permanente.
    Contratos ativos: verifica validade por data.
    """
    dados = _carregar_json(caminho)
    if dados.get("metadata", {}).get("status") != "SUCESSO":
        return False, dados

    # Contrato encerrado — responsáveis não mudam, skip permanente
    if encerrado:
        return True, dados

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
                return (dados if isinstance(dados, list) else [dados]), "SUCESSO"
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
# Helpers
# ---------------------------------------------------------------------------

def _contrato_encerrado(vigencia_fim: str | None) -> bool:
    """Retorna True se a vigência do contrato já passou."""
    if not vigencia_fim:
        return False
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(vigencia_fim[:len(fmt)], fmt).date() < datetime.now().date()
        except ValueError:
            continue
    return False


# ---------------------------------------------------------------------------
# Tarefa individual
# ---------------------------------------------------------------------------

def _processar(t: dict) -> str:
    id_contrato  = t["id_contrato"]
    vigencia_fim = t.get("vigencia_fim")
    encerrado    = _contrato_encerrado(vigencia_fim)

    nome   = os.path.join(_PASTA, f"contratos_responsaveis_{id_contrato}.json")
    valido, _ = _verificar_cache(nome, encerrado=encerrado)

    if valido:
        return f"⏭️ SKIP | {id_contrato}"

    url = f"{_BASE_URL}/contrato/{id_contrato}/responsaveis"
    dados, status = _get(url)

    if status == "SUCESSO" and dados is not None:
        for r in dados:
            r["id_contrato_origem"] = id_contrato

    _salvar(nome, url, dados, status)

    if status == "SUCESSO":
        return f"✅ DONE | {id_contrato} | {len(dados or [])} responsáveis"
    else:
        return f"❌ {status} | {id_contrato}"


# ---------------------------------------------------------------------------
# Montagem da fila — lê contratos do cache temp/contratos/
# ---------------------------------------------------------------------------

def _montar_fila() -> list[dict]:
    """
    Lê todos os JSONs de contratos e monta uma tarefa por contrato único.
    """
    vistos: set = set()
    fila = []

    for arq in sorted(os.listdir(_PASTA_CONTRATOS)):
        if not arq.startswith("contratos_") or not arq.endswith(".json"):
            continue
        dados = _carregar_json(os.path.join(_PASTA_CONTRATOS, arq))
        if dados.get("metadata", {}).get("status") != "SUCESSO":
            continue

        for c in dados.get("respostas", {}).get("resultado", []) or []:
            id_c = c.get("id")
            if not id_c or id_c in vistos:
                continue
            vistos.add(id_c)
            fila.append({
                "id_contrato":  id_c,
                "vigencia_fim": c.get("vigencia_fim"),
            })

    return fila


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def executar() -> int:
    """Extrai responsáveis de todos os contratos. Retorna número de falhas."""
    os.makedirs(_PASTA, exist_ok=True)

    if not os.path.exists(_PASTA_CONTRATOS) or not os.listdir(_PASTA_CONTRATOS):
        print("⚠️  Cache de contratos vazio. Execute extrator_contratos primeiro.")
        return 0

    fila    = _montar_fila()
    total   = len(fila)
    workers = PIPELINE_CONFIG.get("max_workers_responsaveis", 15)

    print(f"   Contratos na fila : {total}")
    print(f"   Validade cache    : {_DIAS_VALIDADE} dia(s)")
    print(f"\n🚀 INICIANDO EXTRAÇÃO DE RESPONSÁVEIS | WORKERS: {workers} | TOTAL: {total}\n")

    if total == 0:
        print("⏭️  Nada a extrair.")
        return 0

    concluidas = erros = skips = ultimo_log_skip = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        try:
            futures = {pool.submit(_processar, t): t for t in fila}
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
