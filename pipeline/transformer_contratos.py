"""
pipeline/transformer_contratos.py
-----------------------------------
Varre temp/contratos/ e gera contratos.csv para Power BI.

Cada linha representa um contrato único.
Deduplicação por id_contrato — mantém o registro com data_extracao
mais recente.

Uso como módulo:
    from pipeline.transformer_contratos import transformar
    transformar()

Uso via CLI:
    python -m pipeline.transformer_contratos
"""

import csv
import glob
import json
import os
import re
import sys
from datetime import datetime
from typing import Optional

from config.config import EXPORT_CONFIG, CONFIG_CONTRATOS

_PASTA_CONTRATOS = CONFIG_CONTRATOS["pasta_cache"]

# ---------------------------------------------------------------------------
# Schema do CSV final
# ---------------------------------------------------------------------------
COLUNAS = [
    # Rastreabilidade
    "arquivo_origem",
    "data_extracao",

    # Identificação
    "id_contrato",
    "id_compra",
    "numero",
    "processo",
    "origem_sigla",
    "origem_uasg",

    # Tipo e modalidade
    "tipo",
    "modalidade",
    "codigo_modalidade",
    "licitacao_numero",
    "unidade_compra",

    # Fornecedor
    "fornecedor_cnpj",
    "fornecedor_nome",

    # Valores
    "valor_global",
    "valor_parcela",

    # Vigência
    "vigencia_inicio",
    "vigencia_fim",
    "prorrogavel",

    # Links úteis
    "link_responsaveis",
]


# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------

def _limpar(texto: Optional[str]) -> str:
    if not texto or str(texto).lower() in ("null", "none"):
        return ""
    t = str(texto)
    t = t.replace('"', '')
    t = t.replace('`', "'")
    t = t.replace('´', '')
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _valor(v) -> str:
    if v is None:
        return ""
    try:
        return f"{float(v):.2f}".replace(".", ",")
    except (ValueError, TypeError):
        return str(v)


def _data(valor: Optional[str]) -> str:
    if not valor:
        return ""
    tentativas = [
        ("%Y-%m-%dT%H:%M:%S", 19),
        ("%Y-%m-%d %H:%M:%S", 19),
        ("%Y-%m-%d",          10),
        ("%d/%m/%Y",          10),
    ]
    for fmt, tam in tentativas:
        try:
            return datetime.strptime(str(valor)[:tam], fmt).strftime("%d/%m/%Y")
        except (ValueError, TypeError):
            continue
    return str(valor)


def _bool_str(v) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "Sim" if v else "Não"
    s = str(v).lower()
    if s in ("true", "1", "sim"):
        return "Sim"
    if s in ("false", "0", "não", "nao"):
        return "Não"
    return str(v)


def _parse_fornecedor(c: dict) -> tuple[str, str]:
    """
    Extrai (cnpj, nome) do objeto fornecedor.
    A API retorna fornecedor como objeto aninhado: {"cnpj": "...", "nome": "..."}
    """
    forn = c.get("fornecedor", {}) or {}
    if isinstance(forn, dict):
        return str(forn.get("cnpj") or "").strip(), _limpar(forn.get("nome"))
    return "", ""


# ---------------------------------------------------------------------------
# Indexação — deduplicação por id_contrato
# ---------------------------------------------------------------------------

def _indexar() -> dict[str, dict]:
    """
    Retorna { id_contrato: registro } — deduplicado pelo mais recente.
    """
    banco: dict[str, dict] = {}

    jsons = sorted(glob.glob(f"{_PASTA_CONTRATOS}/contratos_*.json"))
    if not jsons:
        print(f"⚠️  Nenhum JSON em {_PASTA_CONTRATOS}")
        return banco

    print(f"📂 {len(jsons)} arquivo(s) de contratos encontrado(s). Processando...")

    for caminho in jsons:
        try:
            with open(caminho, encoding="utf-8") as f:
                envelope = json.load(f)
        except Exception as exc:
            print(f"  ⚠️  Erro ao ler {caminho}: {exc}")
            continue

        if envelope.get("metadata", {}).get("status") != "SUCESSO":
            continue

        arquivo = os.path.basename(caminho)
        data_ext = envelope.get("metadata", {}).get("data_extracao", "")

        for reg in envelope.get("respostas", {}).get("resultado", []) or []:
            if not isinstance(reg, dict):
                continue

            id_c = str(reg.get("id") or "")
            if not id_c:
                continue

            reg["_arquivo_origem"] = arquivo
            reg["_data_extracao"] = data_ext

            if id_c not in banco or data_ext > banco[id_c].get("_data_extracao", ""):
                banco[id_c] = reg

    return banco


# ---------------------------------------------------------------------------
# Mapeamento de campos
# ---------------------------------------------------------------------------

def _mapear(reg: dict) -> dict:
    cnpj, nome_forn = _parse_fornecedor(reg)
    id_c = str(reg.get("id") or "")

    # --- GERAÇÃO DO ID_COMPRA (Blindado contra None) ---
    unidade_compra = str(reg.get("unidade_compra") or "")
    codigo_modalidade = str(reg.get("codigo_modalidade") or "")
    numero_compra = str(reg.get("licitacao_numero") or "").replace("/", "")

    # Só gera o ID se houver licitação e NÃO for NAOSEAPLIC, caso contrário deixa vazio
    id_compra = f"{unidade_compra}{codigo_modalidade}{numero_compra}" if numero_compra and codigo_modalidade != "NAOSEAPLIC" else ""

    return {
        "arquivo_origem":     reg.get("_arquivo_origem", ""),
        "data_extracao":      reg.get("_data_extracao", ""),
        "id_contrato":        id_c,
        "id_compra":          id_compra,
        "numero":             reg.get("numero_contrato") or reg.get("numero", ""),
        "processo":           reg.get("processo", ""),
        "origem_sigla":       reg.get("origem_sigla", ""),
        "origem_uasg":        reg.get("origem_uasg", ""),
        "tipo":               reg.get("tipo", ""),
        "modalidade":         reg.get("modalidade", ""),
        "unidade_compra":     unidade_compra,
        "codigo_modalidade":  codigo_modalidade,
        "licitacao_numero":   reg.get("licitacao_numero", ""),
        "fornecedor_cnpj":    cnpj,
        "fornecedor_nome":    nome_forn,
        "valor_global":       _valor(reg.get("valor_global")),
        "valor_parcela":      _valor(reg.get("valor_parcela")),
        "vigencia_inicio":    _data(reg.get("vigencia_inicio")),
        "vigencia_fim":       _data(reg.get("vigencia_fim")),
        "prorrogavel":        _bool_str(reg.get("prorrogavel")),
        "link_responsaveis": (
            f"https://contratos.comprasnet.gov.br/api/contrato/{id_c}/responsaveis"
            if id_c else ""
        ),
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def transformar(
    pasta_contratos: str = _PASTA_CONTRATOS,
    caminho_saida: Optional[str] = None,
) -> None:
    global _PASTA_CONTRATOS
    _PASTA_CONTRATOS = pasta_contratos

    if caminho_saida is None:
        caminho_saida = os.path.join(
            EXPORT_CONFIG["pasta_saida"], "contratos.csv")

    banco = _indexar()

    if not banco:
        print("⚠️  Nenhum contrato válido encontrado.")
        sys.exit(1)

    registros = [_mapear(reg) for reg in banco.values()]
    registros.sort(key=lambda r: (
        r.get("origem_sigla") or "", r.get("numero") or ""))

    os.makedirs(os.path.dirname(caminho_saida) or ".", exist_ok=True)

    with open(caminho_saida, "w", newline="",
              encoding=EXPORT_CONFIG["encoding"]) as f:
        writer = csv.DictWriter(
            f, fieldnames=COLUNAS,
            delimiter=EXPORT_CONFIG["separador"],
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(registros)

    prorrogaveis = sum(1 for r in registros if r["prorrogavel"] == "Sim")

    print(f"\n✅ CSV gerado: {caminho_saida}")
    print(f"   Contratos    : {len(registros)}")
    print(f"   Prorrogáveis : {prorrogaveis}")
    print(f"   Colunas      : {len(COLUNAS)}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Transforma JSONs de contratos em contratos.csv para Power BI."
    )
    parser.add_argument("--pasta", default=_PASTA_CONTRATOS)
    parser.add_argument("--saida", default=os.path.join(
        EXPORT_CONFIG["pasta_saida"], "contratos.csv"))
    args = parser.parse_args()
    transformar(pasta_contratos=args.pasta, caminho_saida=args.saida)
