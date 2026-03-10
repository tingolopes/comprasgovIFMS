"""
exporters.py
------------
Lê todos os arquivos JSON de cache e exporta CSVs consolidados
prontos para consumo no Power BI.

Um CSV por módulo é gerado na pasta de saída configurada em config.py.
"""

import csv
import glob
import json
import os
from typing import Any

from config import EXPORT_CONFIG, CONFIG_APIS
from logger import log_info, log_erro


# ---------------------------------------------------------------------------
# Interface pública
# ---------------------------------------------------------------------------
def exportar_todos() -> None:
    """Ponto de entrada: exporta Legado e Lei 14.133 para CSV."""
    os.makedirs(EXPORT_CONFIG["pasta_saida"], exist_ok=True)

    _exportar_modulo(
        nome="legado",
        pasta_cache=CONFIG_APIS["LEGADO"]["pasta_cache"],
    )
    _exportar_modulo(
        nome="lei14133",
        pasta_cache=CONFIG_APIS["LEI14133"]["pasta_cache"],
    )


# ---------------------------------------------------------------------------
# Internos
# ---------------------------------------------------------------------------
def _exportar_modulo(nome: str, pasta_cache: str) -> None:
    arquivos = glob.glob(f"{pasta_cache}/**/*.json", recursive=True) + \
        glob.glob(f"{pasta_cache}/*.json")

    if not arquivos:
        log_info(
            "Nenhum arquivo JSON encontrado para o módulo '%s'. Pulando exportação.", nome)
        return

    registros: list[dict] = []
    for caminho in arquivos:
        registros.extend(_extrair_registros(caminho))

    if not registros:
        log_info("Módulo '%s': nenhum registro com dados para exportar.", nome)
        return

    caminho_csv = os.path.join(
        EXPORT_CONFIG["pasta_saida"], f"compras_{nome}.csv")
    _escrever_csv(caminho_csv, registros)
    log_info("📄 CSV exportado: %s (%d registros)", caminho_csv, len(registros))


def _extrair_registros(caminho_json: str) -> list[dict]:
    """
    Lê um arquivo de cache e retorna a lista de registros enriquecidos
    com metadados de origem.
    """
    try:
        with open(caminho_json, "r", encoding="utf-8") as f:
            envelope = json.load(f)
    except Exception as exc:
        log_erro("Erro ao ler %s: %s", caminho_json, exc)
        return []

    if envelope.get("metadata", {}).get("status") != "SUCESSO":
        return []

    resultado = envelope.get("respostas", {}).get("resultado", [])
    if not isinstance(resultado, list):
        return []

    # Enriquece cada registro com metadados de rastreabilidade
    url = envelope.get("metadata", {}).get("url_consultada", "")
    data_extracao = envelope.get("metadata", {}).get("data_extracao", "")
    arquivo_origem = os.path.basename(caminho_json)

    enriquecidos = []
    for item in resultado:
        if isinstance(item, dict):
            item["_url_origem"] = url
            item["_data_extracao"] = data_extracao
            item["_arquivo_origem"] = arquivo_origem
            enriquecidos.append(item)

    return enriquecidos


def _escrever_csv(caminho: str, registros: list[dict]) -> None:
    """Escreve lista de dicts em CSV com separador e encoding do Power BI."""
    # Coleta todas as colunas possíveis (union de todas as chaves)
    colunas: list[str] = []
    seen: set[str] = set()
    for r in registros:
        for k in r:
            if k not in seen:
                colunas.append(k)
                seen.add(k)

    # Colunas de metadados sempre por último
    meta_cols = ["_url_origem", "_data_extracao", "_arquivo_origem"]
    colunas = [c for c in colunas if c not in meta_cols] + meta_cols

    with open(caminho, "w", newline="", encoding=EXPORT_CONFIG["encoding"]) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=colunas,
            delimiter=EXPORT_CONFIG["separador"],
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(registros)
