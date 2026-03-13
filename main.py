"""
main.py
-------
Orquestrador do pipeline de extração de compras públicas.

Fluxo completo (padrão):
  1. Extrai compras — módulo Legado         → temp/compras/
  2. Extrai compras — módulo Lei 14.133     → temp/compras/
  3. Consolida JSONs                        → data/compras.csv
  4. Extrai itens de cada compra            → temp/itens/
  5. Consolida itens                        → data/itens.csv
  6. Extrai atas de registro de preço       → temp/atas/
  7. Consolida atas                         → data/atas.csv
  8. Extrai itens das atas                  → temp/atas_itens/
  9. Consolida itens das atas               → data/atas_itens.csv

Modos disponíveis:
  python main.py                                # pipeline completo
  python main.py --modo transformer_compras     # só gera compras.csv
  python main.py --modo extrator_itens          # extrai itens + gera itens.csv
  python main.py --modo transformer_itens       # só gera itens.csv
  python main.py --modo extrator_atas           # extrai atas + gera atas.csv
  python main.py --modo transformer_atas        # só gera atas.csv
  python main.py --modo extrator_atas_itens     # extrai itens das atas + gera atas_itens.csv
  python main.py --modo transformer_atas_itens  # só gera atas_itens.csv
"""

import argparse
import os
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from config.config import CONFIG_APIS, CONFIG_ATAS, EXPORT_CONFIG, PIPELINE_CONFIG
from pipeline.extractors_compras import extrair_legado, extrair_14133
from pipeline.extractors_itens import executar as executar_itens
from pipeline.extractors_atas import executar as executar_atas
from pipeline.extractors_atas_itens import executar as executar_atas_itens
from pipeline.transformer_compras import transformar as transformar_compras
from pipeline.transformer_itens import transformar as transformar_itens
from pipeline.transformer_atas import transformar as transformar_atas
from pipeline.transformer_atas_itens import transformar as transformar_atas_itens
from pipeline.logger import log_info, resumo_skips


# ---------------------------------------------------------------------------
# Motores de extração
# ---------------------------------------------------------------------------

def _executar_motor(nome: str, tarefas: list, fn, workers: int) -> int:
    """
    Motor genérico com progresso em linha, igual ao extrator de itens.
    Exibe skips periodicamente e loga DONE/FAIL linha a linha.
    Retorna o número de falhas.
    """
    total = len(tarefas)
    intervalo_skip = PIPELINE_CONFIG["log_intervalo_skip"]

    log_info("▶ %s — %d tarefas | %d threads", nome, total, workers)

    concluidas = falhas = skips = ultimo_log_skip = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futuros = {pool.submit(fn, *t): t for t in tarefas}
        for futuro in as_completed(futuros):
            concluidas += 1
            res = futuro.result()   # string: "✅ DONE | ...", "⏭️  SKIP | ...", "❌ FAIL | ..."

            if "❌ FAIL" in res:
                falhas += 1
            elif "⏭️" in res:
                skips += 1

            perc = concluidas / total * 100

            if "⏭️" in res:
                # Skips: loga periodicamente
                if (skips - ultimo_log_skip) >= intervalo_skip or concluidas == total:
                    ultimo_log_skip = skips
                    log_info("⏭️  SKIPs acumulados: %d | %d/%d (%.1f%%) | Falhas: %d",
                             skips, concluidas, total, perc, falhas)
            else:
                # DONE e FAIL: loga sempre
                log_info("%s | %d/%d (%.1f%%) | Falhas: %d",
                         res, concluidas, total, perc, falhas)

    log_info("   Concluído — DONE: %d | SKIP: %d | FAIL: %d",
             concluidas - skips - falhas, skips, falhas)
    return falhas


def _executar_legado() -> int:
    cfg = CONFIG_APIS["LEGADO"]
    os.makedirs(cfg["pasta_cache"], exist_ok=True)
    tarefas = [
        (unidade, ano, endpoint)
        for unidade in cfg["uasgs"]
        for ano in cfg["anos"]
        for endpoint in cfg["endpoints"]
    ]
    return _executar_motor(
        "Módulo LEGADO", tarefas, extrair_legado,
        PIPELINE_CONFIG["max_workers_legado"],
    )


def _executar_14133() -> int:
    cfg = CONFIG_APIS["LEI14133"]
    os.makedirs(cfg["pasta_cache"], exist_ok=True)
    tarefas = [
        (unidade, ano, cod_mod, nome_mod)
        for unidade in cfg["uasgs"]
        for ano in cfg["anos"]
        for cod_mod, nome_mod in cfg["modalidades"].items()
    ]
    return _executar_motor(
        "Módulo LEI 14.133", tarefas, extrair_14133,
        PIPELINE_CONFIG["max_workers_14133"],
    )


# ---------------------------------------------------------------------------
# Modos de execução
# ---------------------------------------------------------------------------

def _modo_transformer_compras() -> None:
    log_info("=" * 60)
    log_info("📤 GERANDO compras.csv...")
    transformar_compras(
        pastas=[CONFIG_APIS["LEGADO"]["pasta_cache"]],
        caminho_saida=os.path.join(
            EXPORT_CONFIG["pasta_saida"], "compras.csv"),
    )
    log_info("=" * 60)


def _modo_transformer_itens() -> None:
    log_info("=" * 60)
    log_info("📤 GERANDO itens.csv...")
    transformar_itens(
        pasta_itens="temp/itens",
        caminho_saida=os.path.join(EXPORT_CONFIG["pasta_saida"], "itens.csv"),
    )
    log_info("=" * 60)


def _modo_extrator_itens() -> None:
    log_info("=" * 60)
    log_info("🔩 EXTRAINDO ITENS...")
    falhas = executar_itens()
    _modo_transformer_itens()
    if falhas > 0:
        log_info("⚠️  Extração de itens finalizada com %d falha(s).", falhas)
        sys.exit(1)


def _modo_transformer_atas() -> None:
    log_info("=" * 60)
    log_info("📤 GERANDO atas.csv...")
    transformar_atas(
        pasta_atas=CONFIG_ATAS["pasta_cache"],
        caminho_saida=os.path.join(EXPORT_CONFIG["pasta_saida"], "atas.csv"),
    )
    log_info("=" * 60)


def _modo_extrator_atas() -> None:
    log_info("=" * 60)
    log_info("📋 EXTRAINDO ATAS DE REGISTRO DE PREÇO...")
    falhas = executar_atas()
    _modo_transformer_atas()
    if falhas > 0:
        log_info("⚠️  Extração de atas finalizada com %d falha(s).", falhas)
        sys.exit(1)


def _modo_transformer_atas_itens() -> None:
    log_info("=" * 60)
    log_info("📤 GERANDO atas_itens.csv...")
    transformar_atas_itens(
        pasta_itens=CONFIG_ATAS["pasta_cache_itens"],
        caminho_saida=os.path.join(
            EXPORT_CONFIG["pasta_saida"], "atas_itens.csv"),
    )
    log_info("=" * 60)


def _modo_extrator_atas_itens() -> None:
    log_info("=" * 60)
    log_info("📦 EXTRAINDO ITENS DAS ATAS...")
    falhas = executar_atas_itens()
    _modo_transformer_atas_itens()
    if falhas > 0:
        log_info("⚠️  Extração de itens das atas finalizada com %d falha(s).", falhas)
        sys.exit(1)


def _modo_extrator_compras() -> None:
    log_info("🚀 INICIANDO PIPELINE DE EXTRAÇÃO DE COMPRAS PÚBLICAS")
    log_info("=" * 60)

    falhas_legado = _executar_legado()
    falhas_14133 = _executar_14133()
    falhas_compras = falhas_legado + falhas_14133

    log_info("=" * 60)
    log_info("📊 RESUMO DA EXTRAÇÃO DE COMPRAS")
    log_info("  Falhas Legado   : %d", falhas_legado)
    log_info("  Falhas Lei14133 : %d", falhas_14133)
    log_info("  SKIPs           : %d", resumo_skips())

    _modo_transformer_compras()

    log_info("🔩 INICIANDO EXTRAÇÃO DE ITENS")
    falhas_itens = executar_itens()

    _modo_transformer_itens()

    log_info("📋 INICIANDO EXTRAÇÃO DE ATAS")
    falhas_atas = executar_atas()
    _modo_transformer_atas()

    log_info("📦 INICIANDO EXTRAÇÃO DE ITENS DAS ATAS")
    falhas_atas_itens = executar_atas_itens()
    _modo_transformer_atas_itens()

    falhas_totais = falhas_compras + falhas_itens + falhas_atas + falhas_atas_itens

    log_info("=" * 60)
    log_info("📊 RESUMO FINAL")
    log_info("  Falhas compras     : %d", falhas_compras)
    log_info("  Falhas itens       : %d", falhas_itens)
    log_info("  Falhas atas        : %d", falhas_atas)
    log_info("  Falhas atas_itens  : %d", falhas_atas_itens)

    if falhas_totais > 0:
        log_info("⚠️  Pipeline finalizado com %d falha(s).", falhas_totais)
        sys.exit(1)
    else:
        log_info("🎉 PIPELINE CONCLUÍDO COM SUCESSO!")
        sys.exit(0)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args():
    parser = argparse.ArgumentParser(
        description="Pipeline de extração de compras públicas (dadosabertos.compras.gov.br)",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--modo",
        choices=[
            "extrator_compras",
            "transformer_compras",
            "extrator_itens",
            "transformer_itens",
            "extrator_atas",
            "transformer_atas",
            "extrator_atas_itens",
            "transformer_atas_itens",
        ],
        default="extrator_compras",
        help=(
            "extrator_compras       → pipeline completo (padrão)\n"
            "transformer_compras    → gera compras.csv dos JSONs já baixados\n"
            "extrator_itens         → extrai itens + gera itens.csv\n"
            "transformer_itens      → gera itens.csv dos JSONs já baixados\n"
            "extrator_atas          → extrai atas ARP + gera atas.csv\n"
            "transformer_atas       → gera atas.csv dos JSONs já baixados\n"
            "extrator_atas_itens    → extrai itens das atas + gera atas_itens.csv\n"
            "transformer_atas_itens → gera atas_itens.csv dos JSONs já baixados"
        ),
    )
    return parser.parse_args()


def _limpar_pycache() -> None:
    raiz = os.path.dirname(os.path.abspath(__file__))
    for dirpath, dirnames, _ in os.walk(raiz):
        for d in dirnames:
            if d == "__pycache__":
                shutil.rmtree(os.path.join(dirpath, d), ignore_errors=True)


# ---------------------------------------------------------------------------
# Entrada
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    args = _parse_args()
    try:
        if args.modo == "transformer_compras":
            _modo_transformer_compras()
        elif args.modo == "extrator_itens":
            _modo_extrator_itens()
        elif args.modo == "transformer_itens":
            _modo_transformer_itens()
        elif args.modo == "extrator_atas":
            _modo_extrator_atas()
        elif args.modo == "transformer_atas":
            _modo_transformer_atas()
        elif args.modo == "extrator_atas_itens":
            _modo_extrator_atas_itens()
        elif args.modo == "transformer_atas_itens":
            _modo_transformer_atas_itens()
        else:
            _modo_extrator_compras()
    finally:
        _limpar_pycache()
