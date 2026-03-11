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

Modos disponíveis:
  python main.py                              # pipeline completo
  python main.py --modo transformer_compras   # só gera compras.csv
  python main.py --modo extrator_itens        # extrai itens + gera itens.csv
  python main.py --modo transformer_itens     # só gera itens.csv
"""

import argparse
import os
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from config.config import CONFIG_APIS, EXPORT_CONFIG, PIPELINE_CONFIG
from pipeline.extractors_compras import extrair_legado, extrair_14133
from pipeline.extractors_itens import executar as executar_itens
from pipeline.transformer_compras import transformar as transformar_compras
from pipeline.transformer_itens import transformar as transformar_itens
from pipeline.logger import log_info, resumo_skips


# ---------------------------------------------------------------------------
# Motores de extração
# ---------------------------------------------------------------------------

def _executar_legado() -> int:
    cfg = CONFIG_APIS["LEGADO"]
    os.makedirs(cfg["pasta_cache"], exist_ok=True)

    tarefas = [
        (unidade, ano, endpoint)
        for unidade in cfg["uasgs"]
        for ano in cfg["anos"]
        for endpoint in cfg["endpoints"]
    ]

    log_info("▶ Módulo LEGADO — %d tarefas | %d threads",
             len(tarefas), PIPELINE_CONFIG["max_workers_legado"])

    falhas = 0
    with ThreadPoolExecutor(max_workers=PIPELINE_CONFIG["max_workers_legado"]) as pool:
        futuros = {
            pool.submit(extrair_legado, unidade, ano, endpoint): (unidade, ano, endpoint)
            for unidade, ano, endpoint in tarefas
        }
        for futuro in as_completed(futuros):
            if not futuro.result():
                falhas += 1

    return falhas


def _executar_14133() -> int:
    cfg = CONFIG_APIS["LEI14133"]
    os.makedirs(cfg["pasta_cache"], exist_ok=True)

    tarefas = [
        (unidade, ano, cod_mod, nome_mod)
        for unidade in cfg["uasgs"]
        for ano in cfg["anos"]
        for cod_mod, nome_mod in cfg["modalidades"].items()
    ]

    log_info("▶ Módulo LEI 14.133 — %d tarefas | %d threads",
             len(tarefas), PIPELINE_CONFIG["max_workers_14133"])

    falhas = 0
    with ThreadPoolExecutor(max_workers=PIPELINE_CONFIG["max_workers_14133"]) as pool:
        futuros = {
            pool.submit(extrair_14133, unidade, ano, cod_mod, nome_mod): (unidade, ano, cod_mod, nome_mod)
            for unidade, ano, cod_mod, nome_mod in tarefas
        }
        for futuro in as_completed(futuros):
            if not futuro.result():
                falhas += 1

    return falhas


# ---------------------------------------------------------------------------
# Modos de execução
# ---------------------------------------------------------------------------

def _modo_transformer_compras() -> None:
    log_info("=" * 60)
    log_info("📤 GERANDO compras.csv...")
    transformar_compras(
        pastas=[CONFIG_APIS["LEGADO"]["pasta_cache"]],
        caminho_saida=os.path.join(EXPORT_CONFIG["pasta_saida"], "compras.csv"),
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


def _modo_extrator_compras() -> None:
    log_info("🚀 INICIANDO PIPELINE DE EXTRAÇÃO DE COMPRAS PÚBLICAS")
    log_info("=" * 60)

    falhas_legado  = _executar_legado()
    falhas_14133   = _executar_14133()
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

    falhas_totais = falhas_compras + falhas_itens

    log_info("=" * 60)
    log_info("📊 RESUMO FINAL")
    log_info("  Falhas compras : %d", falhas_compras)
    log_info("  Falhas itens   : %d", falhas_itens)

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
        ],
        default="extrator_compras",
        help=(
            "extrator_compras    → pipeline completo (padrão)\n"
            "transformer_compras → gera compras.csv dos JSONs já baixados\n"
            "extrator_itens      → extrai itens + gera itens.csv\n"
            "transformer_itens   → gera itens.csv dos JSONs já baixados"
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
        else:
            _modo_extrator_compras()
    finally:
        _limpar_pycache()
