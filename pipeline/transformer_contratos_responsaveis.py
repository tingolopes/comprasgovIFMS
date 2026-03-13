"""
pipeline/transformer_contratos_responsaveis.py
------------------------------------------------
Varre temp/contratos_responsaveis/ e gera contratos_responsaveis.csv
para Power BI.

Cada linha representa: (id_contrato × id_responsavel).
Deduplicação por (id_contrato + id_responsavel) — mantém o mais recente.

O campo 'usuario' vem no formato "MATRICULA-NOME" e é separado
em usuario_cpf e usuario_nome para facilitar filtragem.

Uso como módulo:
    from pipeline.transformer_contratos_responsaveis import transformar
    transformar()

Uso via CLI:
    python -m pipeline.transformer_contratos_responsaveis
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

_PASTA_RESPONSAVEIS = CONFIG_CONTRATOS["pasta_cache_responsaveis"]

# ---------------------------------------------------------------------------
# Schema do CSV final
# ---------------------------------------------------------------------------
COLUNAS = [
    # Rastreabilidade
    "arquivo_origem",
    "data_extracao",

    # Identificação
    "id_responsavel",
    "id_contrato",

    # Responsável
    "usuario_cpf",
    "usuario_nome",
    "funcao_id",
    "portaria",
    "situacao",

    # Vigência da responsabilidade
    "data_inicio",
    "data_fim",
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


def _parse_usuario(usuario_str: str) -> tuple[str, str]:
    """
    Extrai (matricula, nome) do campo usuario.
    Formato: "1234567-NOME DO SERVIDOR"
    """
    if not usuario_str:
        return "", ""
    partes = str(usuario_str).split("-", 1)
    matricula = partes[0].strip()
    nome = (_limpar(partes[1]) if len(partes)
            > 1 else "").lstrip("* -").strip()
    return matricula, nome


# ---------------------------------------------------------------------------
# Indexação — deduplicação por (id_contrato + id_responsavel)
# ---------------------------------------------------------------------------

def _indexar() -> dict[str, dict]:
    """
    Retorna { "id_contrato|id_responsavel": registro } — deduplicado.
    """
    banco: dict[str, dict] = {}

    jsons = sorted(
        glob.glob(f"{_PASTA_RESPONSAVEIS}/contratos_responsaveis_*.json"))
    if not jsons:
        print(f"⚠️  Nenhum JSON em {_PASTA_RESPONSAVEIS}")
        return banco

    print(f"📂 {len(jsons)} arquivo(s) de responsáveis encontrado(s). Processando...")

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

            id_resp = str(reg.get("id") or "")
            id_cont = str(reg.get("id_contrato_origem") or "")

            if not id_resp or not id_cont:
                continue

            chave = f"{id_cont}|{id_resp}"
            reg["_arquivo_origem"] = arquivo
            reg["_data_extracao"] = data_ext

            if chave not in banco or data_ext > banco[chave].get("_data_extracao", ""):
                banco[chave] = reg

    return banco


# ---------------------------------------------------------------------------
# Mapeamento de campos
# ---------------------------------------------------------------------------

def _mapear(reg: dict) -> dict:
    cpf, nome_usuario = _parse_usuario(reg.get("usuario", ""))
    return {
        "arquivo_origem":    reg.get("_arquivo_origem", ""),
        "data_extracao":     reg.get("_data_extracao", ""),
        "id_responsavel":    reg.get("id", ""),
        "id_contrato":       reg.get("id_contrato_origem", ""),
        "usuario_cpf":       cpf,
        "usuario_nome":      nome_usuario,
        "funcao_id":         reg.get("funcao_id", ""),
        "portaria":          reg.get("portaria", ""),
        "situacao":          reg.get("situacao", ""),
        "data_inicio":       _data(reg.get("data_inicio")),
        "data_fim":          _data(reg.get("data_fim")),
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def transformar(
    pasta_responsaveis: str = _PASTA_RESPONSAVEIS,
    caminho_saida: Optional[str] = None,
) -> None:
    global _PASTA_RESPONSAVEIS
    _PASTA_RESPONSAVEIS = pasta_responsaveis

    if caminho_saida is None:
        caminho_saida = os.path.join(
            EXPORT_CONFIG["pasta_saida"], "contratos_responsaveis.csv"
        )

    banco = _indexar()

    if not banco:
        print("⚠️  Nenhum responsável válido encontrado.")
        sys.exit(1)

    registros = [_mapear(reg) for reg in banco.values()]
    registros.sort(key=lambda r: (r.get("id_contrato")
                   or "", r.get("id_responsavel") or ""))

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

    ativos = sum(1 for r in registros if not r["data_fim"])

    print(f"\n✅ CSV gerado: {caminho_saida}")
    print(f"   Responsáveis : {len(registros)}")
    print(f"   Ativos       : {ativos}")
    print(f"   Colunas      : {len(COLUNAS)}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Transforma JSONs de responsáveis em contratos_responsaveis.csv."
    )
    parser.add_argument("--pasta", default=_PASTA_RESPONSAVEIS)
    parser.add_argument("--saida", default=os.path.join(
        EXPORT_CONFIG["pasta_saida"], "contratos_responsaveis.csv"))
    args = parser.parse_args()
    transformar(pasta_responsaveis=args.pasta, caminho_saida=args.saida)
