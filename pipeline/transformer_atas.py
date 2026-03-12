"""
pipeline/transformer_atas.py
-----------------------------
Varre temp/atas/ e gera atas.csv consolidado para Power BI.

Deduplicação por numeroControlePncpAta — a mesma ata pode aparecer
em múltiplos arquivos (janelas de vigência de anos diferentes).

Uso como módulo:
    from pipeline.transformer_atas import transformar
    transformar()
"""

import csv
import glob
import json
import os
import re
import sys
from datetime import datetime
from typing import Optional

from config.config import EXPORT_CONFIG, CONFIG_ATAS

_PASTA_ATAS = CONFIG_ATAS["pasta_cache"]

# ---------------------------------------------------------------------------
# Schema do CSV final
# ---------------------------------------------------------------------------
COLUNAS = [
    # Rastreabilidade
    "arquivo_origem",
    "data_extracao",

    # Identificação da ata
    "numero_ata",
    "numero_controle_pncp_ata",
    "numero_controle_pncp_compra",
    "id_compra",

    # Compra origem
    "numero_compra",
    "ano_compra",
    "modalidade_codigo",
    "modalidade_nome",

    # Unidade gerenciadora
    "uasg_codigo",
    "uasg_nome",

    # Vigência e valores
    "data_assinatura",
    "data_vigencia_inicial",
    "data_vigencia_final",
    "valor_total",
    "quantidade_itens",

    # Status e objeto
    "status_ata",
    "ata_excluida",
    "objeto",

    # Links
    "link_ata_pncp",
    "link_compra_pncp",
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
    t = re.sub(r"^- ", "", t)
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


# ---------------------------------------------------------------------------
# Leitura e indexação
# ---------------------------------------------------------------------------

def _indexar() -> dict[str, dict]:
    """
    Retorna { numeroControlePncpAta: registro } — deduplicado.
    Em caso de duplicata mantém o registro com data_extracao mais recente.
    """
    banco: dict[str, dict] = {}

    jsons = sorted(glob.glob(f"{_PASTA_ATAS}/*.json"))
    if not jsons:
        print(f"⚠️  Nenhum JSON em {_PASTA_ATAS}")
        return banco

    print(f"📂 {len(jsons)} arquivo(s) de atas encontrado(s). Processando...")

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

        respostas = envelope.get("respostas", {})
        resultado = respostas.get("resultado", []) or [] if isinstance(
            respostas, dict) else []

        for reg in resultado:
            if not isinstance(reg, dict):
                continue

            chave = str(reg.get("numeroControlePncpAta") or "")
            if not chave:
                continue

            reg["_arquivo_origem"] = arquivo
            reg["_data_extracao"] = data_ext

            # Mantém o mais recente em caso de duplicata
            if chave not in banco:
                banco[chave] = reg
            else:
                existente = banco[chave].get("_data_extracao", "")
                if data_ext > existente:
                    banco[chave] = reg

    return banco


# ---------------------------------------------------------------------------
# Mapeamento de campos
# ---------------------------------------------------------------------------

def _mapear(reg: dict) -> dict:
    return {
        "arquivo_origem":              reg.get("_arquivo_origem", ""),
        "data_extracao":               reg.get("_data_extracao", ""),
        "numero_ata":                  reg.get("numeroAtaRegistroPreco", ""),
        "numero_controle_pncp_ata":    reg.get("numeroControlePncpAta", ""),
        "numero_controle_pncp_compra": reg.get("numeroControlePncpCompra", ""),
        "id_compra":                   reg.get("idCompra", ""),
        "numero_compra":               reg.get("numeroCompra", ""),
        "ano_compra":                  reg.get("anoCompra", ""),
        "modalidade_codigo":           reg.get("codigoModalidadeCompra", ""),
        "modalidade_nome":             reg.get("nomeModalidadeCompra", ""),
        "uasg_codigo":                 reg.get("codigoUnidadeGerenciadora", ""),
        "uasg_nome":                   _limpar(reg.get("nomeUnidadeGerenciadora")),
        "data_assinatura":             _data(reg.get("dataAssinatura")),
        "data_vigencia_inicial":       _data(reg.get("dataVigenciaInicial")),
        "data_vigencia_final":         _data(reg.get("dataVigenciaFinal")),
        "valor_total":                 _valor(reg.get("valorTotal")),
        "quantidade_itens":            str(reg.get("quantidadeItens") or ""),
        "status_ata":                  reg.get("statusAta", ""),
        "ata_excluida":                _bool_str(reg.get("ataExcluido")),
        "objeto":                      _limpar(reg.get("objeto")),
        "link_ata_pncp":               reg.get("linkAtaPNCP", ""),
        "link_compra_pncp":            reg.get("linkCompraPNCP", ""),
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def transformar(
    pasta_atas: str = _PASTA_ATAS,
    caminho_saida: Optional[str] = None,
) -> None:
    global _PASTA_ATAS
    _PASTA_ATAS = pasta_atas

    if caminho_saida is None:
        caminho_saida = os.path.join(EXPORT_CONFIG["pasta_saida"], "atas.csv")

    banco = _indexar()

    if not banco:
        print("⚠️  Nenhuma ata válida encontrada.")
        sys.exit(1)

    registros = [_mapear(reg) for reg in banco.values()]

    # Ordena por data de vigência inicial
    registros.sort(key=lambda r: r.get("data_vigencia_inicial") or "")

    excluidas = sum(1 for r in registros if r["ata_excluida"] == "Sim")

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

    print(f"\n✅ CSV gerado: {caminho_saida}")
    print(f"   Atas únicas   : {len(registros)}")
    print(f"   Atas excluídas: {excluidas}")
    print(f"   Colunas       : {len(COLUNAS)}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Transforma JSONs de atas em atas.csv para Power BI."
    )
    parser.add_argument("--pasta", default=_PASTA_ATAS)
    parser.add_argument("--saida", default=os.path.join(
        EXPORT_CONFIG["pasta_saida"], "atas.csv"))
    args = parser.parse_args()
    transformar(pasta_atas=args.pasta, caminho_saida=args.saida)
