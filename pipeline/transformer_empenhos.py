"""
pipeline/transformer_empenhos.py
---------------------------------
Transforma os JSONs extraídos de empenhos em 3 CSVs para o Power BI:

  data/empenhos.csv          — um registro por empenho
  data/empenhos_itens.csv    — um registro por item de empenho
  data/empenhos_historico.csv — um registro por operação do histórico

Chave de ligação entre os CSVs: codigo_documento
  Formato: {codigo_ug}{gestao}{numero}
  Exemplo: 155849264152025NE000242

Uso como módulo:
    from pipeline.transformer_empenhos import transformar
    transformar()

Uso via CLI:
    python -m pipeline.transformer_empenhos
"""

import csv
import glob
import json
import os
import shutil
import sys
from typing import Optional

from config.config import CONFIG_EMPENHOS, EXPORT_CONFIG, UASGS
from pipeline.logger import log_info, log_erro

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

_PASTA_EMPENHOS  = CONFIG_EMPENHOS["pasta_cache"]
_PASTA_ITENS     = CONFIG_EMPENHOS["pasta_cache_itens"]
_PASTA_HISTORICO = CONFIG_EMPENHOS["pasta_cache_historico"]

_PASTA_SAIDA = EXPORT_CONFIG["pasta_saida"]
_ENCODING    = EXPORT_CONFIG["encoding"]
_SEP         = EXPORT_CONFIG["separador"]

# Mapa codigo_ug → sigla para enriquecer o campo unidade_codigo
_MAPA_SIGLAS = {u["codigo"]: u["sigla"] for u in UASGS}

# ---------------------------------------------------------------------------
# Schemas dos CSVs
# ---------------------------------------------------------------------------

COLUNAS_EMPENHOS = [
    "codigo_documento",
    "id",
    "numero",
    "data_emissao",
    "unidade_codigo",
    "unidade_nome",
    "gestao",
    "fornecedor_documento",
    "fornecedor_nome",
    "fonte",
    "ptres",
    "modalidade_licitacao_siafi",
    "naturezadespesa_codigo",
    "naturezadespesa_nome",
    "empenhado",
    "aliquidar",
    "liquidado",
    "pago",
    "rpinscrito",
    "rpaliquidar",
    "rpaliquidado",
    "rppago",
    "informacao_complementar",
    "sistema_origem",
    "arquivo_origem",
    "data_extracao",
]

COLUNAS_ITENS = [
    "codigo_documento",
    "sequencial",
    "descricao",
    "codigo_subelemento",
    "descricao_subelemento",
    "valor_atual",
    "arquivo_origem",
    "data_extracao",
]

COLUNAS_HISTORICO = [
    "codigo_documento",
    "sequencial",
    "data",
    "operacao",
    "quantidade",
    "valor_unitario",
    "valor_total",
    "arquivo_origem",
    "data_extracao",
]


# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------

def _limpar(texto: Optional[str]) -> str:
    if not texto or str(texto).lower() in ("null", "none"):
        return ""
    return str(texto).strip()


def _split_campo(valor: str, separador: str = " - ", indice: int = 0) -> str:
    """Divide campos como '155849 - CAMPUS NAVIRAI' e retorna a parte desejada."""
    partes = valor.split(separador, 1)
    if indice < len(partes):
        return partes[indice].strip()
    return valor.strip()


def _codigo_documento(codigo_ug: str, gestao: str, numero: str) -> str:
    return f"{codigo_ug}{gestao}{numero}"


def _salvar_csv(caminho: str, colunas: list, registros: list) -> None:
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    with open(caminho, "w", newline="", encoding=_ENCODING) as f:
        writer = csv.DictWriter(f, fieldnames=colunas, delimiter=_SEP, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(registros)


# ---------------------------------------------------------------------------
# Módulo 1 — Empenhos
# ---------------------------------------------------------------------------

def _transformar_empenhos() -> tuple[list, int]:
    """Lê todos os JSONs de empenhos e retorna (registros, total_falhas)."""
    jsons = sorted(glob.glob(os.path.join(_PASTA_EMPENHOS, "*.json")))
    log_info("📂 Empenhos: %d arquivo(s) encontrado(s)", len(jsons))

    registros = []
    falhas    = 0

    for caminho in jsons:
        arquivo = os.path.basename(caminho)
        try:
            with open(caminho, encoding="utf-8") as f:
                envelope = json.load(f)
        except Exception as exc:
            log_erro("Erro ao ler %s: %s", arquivo, exc)
            falhas += 1
            continue

        if envelope.get("metadata", {}).get("status") != "SUCESSO":
            continue

        data_ext  = envelope.get("metadata", {}).get("data_extracao", "")
        empenhos  = envelope.get("respostas", []) or []

        for emp in empenhos:
            numero = _limpar(emp.get("numero", ""))
            gestao = _limpar(emp.get("gestao", ""))
            unidade_raw = _limpar(emp.get("unidade", ""))
            codigo_ug   = _split_campo(unidade_raw, " - ", 0)
            unidade_nome = _split_campo(unidade_raw, " - ", 1)

            fornecedor_raw  = _limpar(emp.get("fornecedor", ""))
            fornecedor_doc  = _split_campo(fornecedor_raw, " - ", 0)
            fornecedor_nome = _split_campo(fornecedor_raw, " - ", 1)

            nat_raw     = _limpar(emp.get("naturezadespesa", ""))
            nat_codigo  = _split_campo(nat_raw, " - ", 0)
            nat_nome    = _split_campo(nat_raw, " - ", 1)

            registros.append({
                "codigo_documento":          _codigo_documento(codigo_ug, gestao, numero),
                "id":                        emp.get("id", ""),
                "numero":                    numero,
                "data_emissao":              _limpar(emp.get("data_emissao", "")),
                "unidade_codigo":            codigo_ug,
                "unidade_nome":              unidade_nome,
                "gestao":                    gestao,
                "fornecedor_documento":      fornecedor_doc,
                "fornecedor_nome":           fornecedor_nome,
                "fonte":                     _limpar(emp.get("fonte", "")),
                "ptres":                     _limpar(emp.get("ptres", "")),
                "modalidade_licitacao_siafi": _limpar(emp.get("modalidade_licitacao_siafi", "")),
                "naturezadespesa_codigo":    nat_codigo,
                "naturezadespesa_nome":      nat_nome,
                "empenhado":                 _limpar(emp.get("empenhado", "")),
                "aliquidar":                 _limpar(emp.get("aliquidar", "")),
                "liquidado":                 _limpar(emp.get("liquidado", "")),
                "pago":                      _limpar(emp.get("pago", "")),
                "rpinscrito":                _limpar(emp.get("rpinscrito", "")),
                "rpaliquidar":               _limpar(emp.get("rpaliquidar", "")),
                "rpaliquidado":              _limpar(emp.get("rpaliquidado", "")),
                "rppago":                    _limpar(emp.get("rppago", "")),
                "informacao_complementar":   _limpar(emp.get("informacao_complementar", "")),
                "sistema_origem":            _limpar(emp.get("sistema_origem", "")),
                "arquivo_origem":            arquivo,
                "data_extracao":             data_ext,
            })

    return registros, falhas


# ---------------------------------------------------------------------------
# Módulo 2 — Itens
# ---------------------------------------------------------------------------

def _transformar_itens() -> tuple[list, int]:
    """Lê todos os JSONs de itens e retorna (registros, total_falhas)."""
    jsons = sorted(glob.glob(os.path.join(_PASTA_ITENS, "*.json")))
    log_info("📂 Itens: %d arquivo(s) encontrado(s)", len(jsons))

    registros = []
    falhas    = 0

    for caminho in jsons:
        arquivo = os.path.basename(caminho)
        try:
            with open(caminho, encoding="utf-8") as f:
                envelope = json.load(f)
        except Exception as exc:
            log_erro("Erro ao ler %s: %s", arquivo, exc)
            falhas += 1
            continue

        if envelope.get("metadata", {}).get("status") != "SUCESSO":
            continue

        data_ext = envelope.get("metadata", {}).get("data_extracao", "")
        itens    = envelope.get("respostas", []) or []

        for item in itens:
            registros.append({
                "codigo_documento":   _limpar(item.get("codigoItemEmpenho", "")),
                "sequencial":         item.get("sequencial", ""),
                "descricao":          _limpar(item.get("descricao", "")),
                "codigo_subelemento": _limpar(item.get("codigoSubelemento", "")),
                "descricao_subelemento": _limpar(item.get("descricaoSubelemento", "")),
                "valor_atual":        _limpar(item.get("valorAtual", "")),
                "arquivo_origem":     arquivo,
                "data_extracao":      data_ext,
            })

    return registros, falhas


# ---------------------------------------------------------------------------
# Módulo 3 — Histórico
# ---------------------------------------------------------------------------

def _transformar_historico() -> tuple[list, int]:
    """Lê todos os JSONs de histórico e retorna (registros, total_falhas)."""
    jsons = sorted(glob.glob(os.path.join(_PASTA_HISTORICO, "*.json")))
    log_info("📂 Histórico: %d arquivo(s) encontrado(s)", len(jsons))

    registros = []
    falhas    = 0

    for caminho in jsons:
        arquivo  = os.path.basename(caminho)
        # Extrai sequencial do nome do arquivo: historico_NV_<codigo_doc>_seq<N>.json
        try:
            seq_parte  = arquivo.rsplit("_seq", 1)[-1].replace(".json", "")
            sequencial = int(seq_parte)
        except ValueError:
            sequencial = ""

        # Extrai codigo_documento do nome do arquivo
        try:
            # formato: historico_<SIGLA>_<codigo_doc>_seq<N>.json
            sem_prefixo = arquivo.replace("historico_", "", 1)
            # remove _seq<N>.json do final
            codigo_doc  = sem_prefixo.rsplit("_seq", 1)[0]
            # remove a sigla do início (ex: "NV_")
            codigo_doc  = codigo_doc.split("_", 1)[-1]
        except Exception:
            codigo_doc = ""

        try:
            with open(caminho, encoding="utf-8") as f:
                envelope = json.load(f)
        except Exception as exc:
            log_erro("Erro ao ler %s: %s", arquivo, exc)
            falhas += 1
            continue

        if envelope.get("metadata", {}).get("status") != "SUCESSO":
            continue

        data_ext   = envelope.get("metadata", {}).get("data_extracao", "")
        operacoes  = envelope.get("respostas", []) or []

        for op in operacoes:
            registros.append({
                "codigo_documento": codigo_doc,
                "sequencial":       sequencial,
                "data":             _limpar(op.get("data", "")),
                "operacao":         _limpar(op.get("operacao", "")),
                "quantidade":       _limpar(op.get("quantidade", "")),
                "valor_unitario":   _limpar(op.get("valorUnitario", "")),
                "valor_total":      _limpar(op.get("valorTotal", "")),
                "arquivo_origem":   arquivo,
                "data_extracao":    data_ext,
            })

    return registros, falhas


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def transformar() -> None:
    log_info("╔══════════════════════════════════════════════════════════╗")
    log_info("║         TRANSFORMER DE EMPENHOS — IFMS                   ║")
    log_info("╚══════════════════════════════════════════════════════════╝")

    saida_empenhos   = os.path.join(_PASTA_SAIDA, "empenhos.csv")
    saida_itens      = os.path.join(_PASTA_SAIDA, "empenhos_itens.csv")
    saida_historico  = os.path.join(_PASTA_SAIDA, "empenhos_historico.csv")

    # --- Empenhos ---
    reg_empenhos, falhas_emp = _transformar_empenhos()
    _salvar_csv(saida_empenhos, COLUNAS_EMPENHOS, reg_empenhos)
    log_info("✅ empenhos.csv        : %d registros | Falhas: %d", len(reg_empenhos), falhas_emp)

    # --- Itens ---
    reg_itens, falhas_itens = _transformar_itens()
    _salvar_csv(saida_itens, COLUNAS_ITENS, reg_itens)
    log_info("✅ empenhos_itens.csv  : %d registros | Falhas: %d", len(reg_itens), falhas_itens)

    # --- Histórico ---
    reg_hist, falhas_hist = _transformar_historico()
    _salvar_csv(saida_historico, COLUNAS_HISTORICO, reg_hist)
    log_info("✅ empenhos_historico.csv: %d registros | Falhas: %d", len(reg_hist), falhas_hist)

    log_info("🏁 Concluído! CSVs gerados em '%s'", _PASTA_SAIDA)


def _limpar_pycache() -> None:
    raiz = os.path.dirname(os.path.abspath(__file__))
    for dirpath, dirnames, _ in os.walk(raiz):
        for d in dirnames:
            if d == "__pycache__":
                shutil.rmtree(os.path.join(dirpath, d), ignore_errors=True)

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    transformar()
    _limpar_pycache()
    sys.exit(0)