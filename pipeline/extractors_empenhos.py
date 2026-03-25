"""
pipeline/extractor_empenhos.py
-------------------------------
Extrai empenhos e seus detalhes para todas as UASGs a partir de 2021.

Fluxo por UASG/ano:
  1. Busca lista de empenhos          → temp/empenhos/
  2. Para cada empenho, busca itens   → temp/empenhos_itens/
  3. Para cada item, busca histórico
     sequencial por sequencial        → temp/empenhos_historico/

Cache diário: arquivos extraídos hoje são pulados automaticamente.

Construção do codigoDocumento (Portal Transparência):
  {codigo_ug}{gestao}{numero_empenho}
  Exemplo: 155849 + 26415 + 2026NE000008 → 155849264152026NE000008

Uso como módulo:
    from pipeline.extractor_empenhos import executar
    falhas = executar()

Uso via CLI:
    python -m pipeline.extractor_empenhos
"""

import json
import os
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests

from config.config import CONFIG_EMPENHOS, PIPELINE_CONFIG, HTTP_HEADERS
from pipeline.logger import log_info, log_erro, log_skip, resumo_skips, resetar_skips

# ---------------------------------------------------------------------------
# Configuração local
# ---------------------------------------------------------------------------
_CFG = CONFIG_EMPENHOS
_ANOS = _CFG["anos"]
_UASGS = _CFG["uasgs"]

_PASTA_EMPENHOS  = _CFG["pasta_cache"]
_PASTA_ITENS     = _CFG["pasta_cache_itens"]
_PASTA_HISTORICO = _CFG["pasta_cache_historico"]

_TOKEN = _CFG["token"]

# Headers para o Portal da Transparência (requer token)
_HEADERS_TRANSPARENCIA = {
    **HTTP_HEADERS,
    "chave-api-dados": _TOKEN,
}

# ---------------------------------------------------------------------------
# Cache semanal
# ---------------------------------------------------------------------------

_DIAS_CACHE = PIPELINE_CONFIG.get("dias_validade_cache_empenhos", 7)


def _cache_valido(caminho: str) -> bool:
    """Retorna True se o arquivo existe, tem status SUCESSO e foi extraído há menos de 7 dias."""
    if not os.path.exists(caminho):
        return False
    try:
        with open(caminho, encoding="utf-8") as f:
            dados = json.load(f)
        status   = dados.get("metadata", {}).get("status", "")
        data_str = dados.get("metadata", {}).get("data_extracao", "")
        if status != "SUCESSO" or not data_str:
            return False
        data_ext   = datetime.strptime(data_str, "%Y-%m-%d %H:%M:%S")
        dias_cache = (datetime.now() - data_ext).days
        return dias_cache < _DIAS_CACHE
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------

def _salvar(caminho: str, url: str, conteudo, status: str = "SUCESSO") -> None:
    envelope = {
        "metadata": {
            "url_consultada": url,
            "data_extracao":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status":         status,
        },
        "respostas": conteudo if conteudo is not None else {},
    }
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(envelope, f, ensure_ascii=False, indent=4)


def _get(url: str, headers: dict = None, params: dict = None) -> tuple[any, str]:
    """HTTP GET com backoff exponencial. Retorna (dados, status)."""
    atraso = PIPELINE_CONFIG["backoff_inicial"]
    hdrs = headers or HTTP_HEADERS

    for tentativa in range(1, PIPELINE_CONFIG["backoff_tentativas"] + 1):
        try:
            resp = requests.get(
                url,
                headers=hdrs,
                params=params,
                timeout=PIPELINE_CONFIG["timeout_segundos"],
            )
            if resp.status_code == 200:
                return resp.json(), "SUCESSO"
            elif resp.status_code == 404:
                return None, "VAZIO"   # sequencial inexistente — fim da série
            elif resp.status_code == 429:
                time.sleep(15 * tentativa)
                continue
            else:
                return None, f"ERRO_{resp.status_code}"
        except Exception as exc:
            log_erro("Tentativa %d (%s): %s", tentativa, url, exc)
        time.sleep(atraso)
        atraso *= 2

    return None, "FALHA"


def _codigo_documento(codigo_ug: str, gestao: str, numero: str) -> str:
    """Monta o codigoDocumento para o Portal da Transparência."""
    return f"{codigo_ug}{gestao}{numero}"


# ---------------------------------------------------------------------------
# Etapa 1 — Empenhos por UASG/ano
# ---------------------------------------------------------------------------

def _extrair_empenhos(uasg: dict, ano: int) -> tuple[list, str]:
    """
    Busca a lista de empenhos da UASG no ano.
    Retorna (lista_empenhos, status).
    """
    sigla   = uasg["sigla"]
    codigo  = uasg["codigo"]
    caminho = os.path.join(_PASTA_EMPENHOS, f"empenhos_{sigla}_{ano}.json")

    if _cache_valido(caminho):
        try:
            with open(caminho, encoding="utf-8") as f:
                dados = json.load(f).get("respostas", [])
            if isinstance(dados, list):
                log_skip()
                return dados, "SKIP"
        except Exception:
            pass

    url = (
        _CFG["base_url_comprasnet"]
        + _CFG["path_empenhos"].format(ano=ano, codigo=codigo)
    )

    dados, status = _get(url)
    _salvar(caminho, url, dados, status)

    if status == "SUCESSO" and isinstance(dados, list):
        return dados, "SUCESSO"
    return [], status


# ---------------------------------------------------------------------------
# Etapa 2 — Itens do empenho
# ---------------------------------------------------------------------------

def _extrair_itens(codigo_doc: str, sigla: str) -> tuple[list, str]:
    """
    Busca os itens de um empenho pelo codigoDocumento.
    Retorna (lista_itens, status).
    """
    caminho = os.path.join(_PASTA_ITENS, f"itens_{sigla}_{codigo_doc}.json")

    if _cache_valido(caminho):
        try:
            with open(caminho, encoding="utf-8") as f:
                dados = json.load(f).get("respostas", [])
            if isinstance(dados, list):
                log_skip()
                return dados, "SKIP"
        except Exception:
            pass

    url    = _CFG["base_url_transparencia"] + _CFG["path_itens"]
    params = {"codigoDocumento": codigo_doc, "pagina": 1}
    url_log = url + f"?codigoDocumento={codigo_doc}&pagina=1"

    dados, status = _get(url, headers=_HEADERS_TRANSPARENCIA, params=params)
    _salvar(caminho, url_log, dados, status)

    if status == "SUCESSO" and isinstance(dados, list):
        return dados, "SUCESSO"
    return [], status


# ---------------------------------------------------------------------------
# Etapa 3 — Histórico por sequencial
# ---------------------------------------------------------------------------

def _extrair_historico(codigo_doc: str, sequencial: int, sigla: str) -> str:
    """
    Busca o histórico de um item pelo sequencial.
    Retorna status final.
    """
    caminho = os.path.join(
        _PASTA_HISTORICO, f"historico_{sigla}_{codigo_doc}_seq{sequencial}.json"
    )

    if _cache_valido(caminho):
        log_skip()
        return "SKIP"

    url    = _CFG["base_url_transparencia"] + _CFG["path_historico"]
    params = {"codigoDocumento": codigo_doc, "sequencial": sequencial, "pagina": 1}
    url_log = url + f"?codigoDocumento={codigo_doc}&sequencial={sequencial}&pagina=1"

    dados, status = _get(url, headers=_HEADERS_TRANSPARENCIA, params=params)

    # Lista vazia ou 404 — fim normal da série de sequenciais
    if status == "VAZIO" or (status == "SUCESSO" and isinstance(dados, list) and not dados):
        return "VAZIO"

    _salvar(caminho, url_log, dados, status)
    return status


# ---------------------------------------------------------------------------
# Tarefa individual por UASG/ano
# ---------------------------------------------------------------------------

def _processar(t: dict) -> str:
    """Processa uma tarefa {uasg, ano}. Retorna string de log."""
    uasg   = t["uasg"]
    ano    = t["ano"]
    sigla  = uasg["sigla"]
    codigo = uasg["codigo"]

    # --- Etapa 1: empenhos ---
    empenhos, status = _extrair_empenhos(uasg, ano)

    if status == "SKIP":
        return f"⏭️  SKIP | {sigla} | {ano} | cache de hoje aproveitado"

    if status != "SUCESSO":
        return f"❌ {status} | {sigla} | {ano} | empenhos"

    if not empenhos:
        return f"✅ DONE | {sigla} | {ano} | 0 empenhos"

    total_emp        = len(empenhos)
    erros_itens      = 0
    total_itens      = 0
    total_historicos = 0

    for idx, emp in enumerate(empenhos, 1):
        numero = emp.get("numero", "")
        gestao = emp.get("gestao", "")
        if not numero or not gestao:
            continue

        codigo_doc = _codigo_documento(codigo, gestao, numero)

        # Log intermediário a cada 10 empenhos para mostrar progresso na tela
        if idx % 10 == 0 or idx == total_emp:
            log_info("🔄 %s | %s | empenho %d/%d | itens: %d | hist: %d",
                     sigla, ano, idx, total_emp, total_itens, total_historicos)

        # --- Etapa 2: itens ---
        itens, status_itens = _extrair_itens(codigo_doc, sigla)
        time.sleep(1)  # respeita limite de 1 req/s do Portal da Transparência
        if status_itens not in ("SUCESSO", "SKIP"):
            erros_itens += 1
            continue

        total_itens += len(itens)

        # --- Etapa 3: histórico por sequencial ---
        for item in itens:
            seq = item.get("sequencial")
            if seq is None:
                continue

            s = int(seq)
            while True:
                status_hist = _extrair_historico(codigo_doc, s, sigla)
                time.sleep(1)  # respeita limite de 1 req/s do Portal da Transparência
                if status_hist == "VAZIO":
                    break
                if status_hist in ("SUCESSO", "SKIP"):
                    total_historicos += 1
                    s += 1
                else:
                    break  # erro HTTP — para sequenciais deste item

    return (
        f"✅ DONE | {sigla} | {ano} | "
        f"{total_emp} emp | {total_itens} itens | {total_historicos} hist"
    )


# ---------------------------------------------------------------------------
# Montagem da fila
# ---------------------------------------------------------------------------

def _montar_fila() -> list[dict]:
    """Uma tarefa por UASG/ano."""
    return [
        {"uasg": uasg, "ano": ano}
        for uasg in _UASGS
        for ano in _ANOS
    ]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def executar() -> int:
    """Extrai empenhos, itens e históricos. Retorna número de falhas."""

    if not _TOKEN:
        log_erro("PORTAL_TRANSPARENCIA_TOKEN não definido no .env — abortando.")
        return 1

    for pasta in [_PASTA_EMPENHOS, _PASTA_ITENS, _PASTA_HISTORICO]:
        os.makedirs(pasta, exist_ok=True)

    resetar_skips()
    fila  = _montar_fila()
    total = len(fila)

    log_info("UASGs     : %d", len(_UASGS))
    log_info("Anos      : %s–%s", _ANOS[0], _ANOS[-1])
    log_info("Tarefas   : %d", total)

    workers = PIPELINE_CONFIG.get("max_workers_empenhos", 1)
    log_info("🚀 INICIANDO EXTRAÇÃO DE EMPENHOS | WORKERS: %d | TOTAL: %d", workers, total)

    concluidas = erros = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        try:
            futures = {pool.submit(_processar, t): t for t in fila}
            for future in as_completed(futures):
                concluidas += 1
                res = future.result()

                if "❌" in res or "FALHA" in res:
                    erros += 1
                    log_erro("%s | %d/%d (%.1f%%) | Falhas: %d",
                             res, concluidas, total, concluidas / total * 100, erros)
                else:
                    log_info("%s | %d/%d (%.1f%%) | Falhas: %d",
                             res, concluidas, total, concluidas / total * 100, erros)

        except KeyboardInterrupt:
            log_info("🛑 Interrompido pelo usuário.")
            pool.shutdown(wait=False, cancel_futures=True)
            sys.exit(0)

    log_info("✅ FIM | Falhas: %d | SKIPs: %d", erros, resumo_skips())
    return erros

def _limpar_pycache() -> None:
    raiz = os.path.dirname(os.path.abspath(__file__))
    for dirpath, dirnames, _ in os.walk(raiz):
        for d in dirnames:
            if d == "__pycache__":
                shutil.rmtree(os.path.join(dirpath, d), ignore_errors=True)

if __name__ == "__main__":
    _limpar_pycache()
    sys.exit(0 if executar() == 0 else 1)