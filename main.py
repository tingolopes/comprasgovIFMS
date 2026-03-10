"""
main.py
-------
Orquestrador do pipeline de extração de compras públicas.

Fluxo:
  1. Extrai dados do módulo Legado (multi-thread)
  2. Extrai dados do módulo Lei 14.133 / PNCP (multi-thread)
  3. Consolida todos os JSONs em um único CSV (compras.csv) para o Power BI
  4. Exibe resumo e encerra com código de saída adequado
"""

import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import CONFIG_APIS, PIPELINE_CONFIG, EXPORT_CONFIG
from extractors import extrair_legado, extrair_14133
from transformer import transformar
from logger import log_info, resumo_skips


# ---------------------------------------------------------------------------
# Motores de extração
# ---------------------------------------------------------------------------
def _executar_legado() -> int:
    """Submete todas as tarefas do módulo Legado ao pool de threads."""
    cfg = CONFIG_APIS["LEGADO"]
    os.makedirs(cfg["pasta_cache"], exist_ok=True)
    falhas = 0

    tarefas = [
        (unidade, ano, endpoint)
        for unidade in cfg["uasgs"]
        for ano in cfg["anos"]
        for endpoint in cfg["endpoints"]
    ]

    log_info("▶ Módulo LEGADO — %d tarefas | %d threads", len(
        tarefas), PIPELINE_CONFIG["max_workers_legado"])

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
    """Submete todas as tarefas do módulo Lei 14.133 ao pool de threads."""
    cfg = CONFIG_APIS["LEI14133"]
    os.makedirs(cfg["pasta_cache"], exist_ok=True)
    falhas = 0

    tarefas = [
        (unidade, ano, cod_mod, nome_mod)
        for unidade in cfg["uasgs"]
        for ano in cfg["anos"]
        for cod_mod, nome_mod in cfg["modalidades"].items()
    ]

    log_info("▶ Módulo LEI 14.133 — %d tarefas | %d threads",
             len(tarefas), PIPELINE_CONFIG["max_workers_14133"])

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
# Ponto de entrada
# ---------------------------------------------------------------------------
def main() -> None:
    log_info("🚀 INICIANDO PIPELINE DE EXTRAÇÃO DE COMPRAS PÚBLICAS")
    log_info("=" * 60)

    falhas_legado = _executar_legado()
    falhas_14133 = _executar_14133()
    falhas_totais = falhas_legado + falhas_14133

    log_info("=" * 60)
    log_info("📊 RESUMO DA EXTRAÇÃO")
    log_info("  Falhas Legado   : %d", falhas_legado)
    log_info("  Falhas Lei 14133: %d", falhas_14133)
    log_info("  SKIPs aproveitados: %d", resumo_skips())

    # Consolidação em CSV único para Power BI
    log_info("=" * 60)
    log_info("📤 GERANDO compras.csv CONSOLIDADO...")
    transformar(
        pastas=[
            CONFIG_APIS["LEGADO"]["pasta_cache"],
            CONFIG_APIS["LEI14133"]["pasta_cache"],
        ],
        caminho_saida=os.path.join(
            EXPORT_CONFIG["pasta_saida"], "compras.csv"),
    )

    log_info("=" * 60)
    if falhas_totais > 0:
        log_info(
            "⚠️  Pipeline finalizado com %d falha(s) pendente(s).", falhas_totais)
        sys.exit(1)
    else:
        log_info("🎉 PIPELINE CONCLUÍDO COM SUCESSO!")
        sys.exit(0)


if __name__ == "__main__":
    main()
