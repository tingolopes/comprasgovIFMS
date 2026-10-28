"""
pipeline/transformer_atas_saldos.py
-------------------------------------
Varre temp/atas_saldos/ e gera atas_saldos.csv para Power BI.

Cada linha representa: (numeroAta × numeroItem × unidade).
Deduplicação por (numero_ata + numero_item + codigo_unidade) —
mantém o registro com data_extracao mais recente (dado dinâmico).

Uso como módulo:
    from pipeline.transformer_atas_saldos import transformar
    transformar()

Uso via CLI:
    python -m pipeline.transformer_atas_saldos
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

_PASTA_SALDOS = CONFIG_ATAS["pasta_cache_saldos"]

# ---------------------------------------------------------------------------
# Schema do CSV final
# ---------------------------------------------------------------------------
COLUNAS = [
    # Rastreabilidade
    "arquivo_origem",
    "data_extracao",

    # Identificação
    "numero_ata",
    "numero_item",

    # Unidade
    "codigo_unidade",
    "nome_unidade",
    "tipo_participacao",

    # Quantidades e saldo
    "quantidade_registrada",
    "quantidade_empenhada",
    "saldo_empenho",

    # Datas
    "data_inclusao",
    "data_atualizacao",
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


def _to_float(v) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    s = s.replace(" ", "").replace("\u00A0", "").replace(",", ".")
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


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


def _parse_unidade(unidade_str: str) -> tuple[str, str]:
    """
    Extrai (codigo, nome) do campo 'unidade' do saldo legado.
    Formato: "153164 - UNIVERSIDADE FEDERAL DE SANTA MARIA"
    """
    if not unidade_str:
        return "", ""
    partes = str(unidade_str).split(" - ", 1)
    codigo = partes[0].strip()
    nome = _limpar(partes[1]) if len(partes) > 1 else ""
    return codigo, nome


# ---------------------------------------------------------------------------
# Indexação — deduplicação por (numero_ata + numero_item + codigo_unidade)
# ---------------------------------------------------------------------------

def _extrair_numero_ata_do_arquivo(nome_arq: str) -> tuple[str, str]:
    """
    Extrai (numero_ata, numero_item) do nome do arquivo.
    'atas_saldos_RT_00001_2023_00001_p1.json' → ('00001/2023', '00001')
    Suporta formato legado: 'atas_saldos_RT_00001_2023_p1.json' → ('00001/2023', '')
    """
    base = nome_arq.replace(".json", "")
    partes = base.split("_")
    try:
        if len(partes) >= 7:
            num_ata = f"{partes[-4]}/{partes[-3]}"
            num_item = partes[-2]
            return num_ata, num_item
        elif len(partes) >= 6:
            num_ata = f"{partes[-3]}/{partes[-2]}"
            return num_ata, ""
    except IndexError:
        pass
    return "", ""


def _indexar() -> dict[str, dict]:
    """
    Retorna { "numero_ata|numero_item|codigo_unidade": registro } — deduplicado.
    Em caso de duplicata mantém o registro com data_extracao mais recente.
    """
    banco: dict[str, dict] = {}

    jsons = sorted(glob.glob(f"{_PASTA_SALDOS}/*.json"))
    if not jsons:
        print(f"⚠️  Nenhum JSON em {_PASTA_SALDOS}")
        return banco

    print(f"📂 {len(jsons)} arquivo(s) de saldos encontrado(s). Processando...")

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
        ata_arq, item_arq = _extrair_numero_ata_do_arquivo(arquivo)

        respostas = envelope.get("respostas", {})
        resultado = (
            respostas.get("resultado", []) or []
            if isinstance(respostas, dict) else []
        )

        for reg in resultado:
            if not isinstance(reg, dict):
                continue

            num_item = str(reg.get("numeroItem") or item_arq or "").strip().zfill(5)
            
            # Suporta tanto 3_consultarUnidadesItem quanto o legado 4_consultarEmpenhosSaldoItem
            cod_un = str(reg.get("codigoUnidade") or "").strip()
            if not cod_un and "unidade" in reg:
                cod_un, _ = _parse_unidade(reg.get("unidade", ""))

            num_ata = str(reg.get("numeroAta") or reg.get("numeroAtaRegistroPreco") or ata_arq or "").strip()

            if not num_item or not cod_un or not num_ata:
                continue

            chave = f"{num_ata}|{num_item}|{cod_un}"
            reg["_arquivo_origem"] = arquivo
            reg["_data_extracao"] = data_ext
            reg["_numero_ata"] = num_ata
            reg["_numero_item"] = num_item

            if chave not in banco:
                banco[chave] = reg
            else:
                if data_ext > banco[chave].get("_data_extracao", ""):
                    banco[chave] = reg

    return banco


# ---------------------------------------------------------------------------
# Mapeamento de campos
# ---------------------------------------------------------------------------

def _mapear(reg: dict) -> dict:
    cod_un = str(reg.get("codigoUnidade") or "").strip()
    nome_un = _limpar(reg.get("nomeUnidade"))
    if not cod_un and "unidade" in reg:
        cod_un, nome_un = _parse_unidade(reg.get("unidade", ""))

    tipo_part = str(reg.get("tipoUnidade") or reg.get("tipo") or "")

    qtd_reg_num = _to_float(reg.get("quantidadeRegistrada"))
    
    # Em 3_consultarUnidadesItem o saldo de empenho é o saldoRemanejamentoEmpenho
    if "saldoRemanejamentoEmpenho" in reg:
        saldo_emp_num = _to_float(reg.get("saldoRemanejamentoEmpenho"))
    else:
        saldo_emp_num = _to_float(reg.get("saldoEmpenho"))

    if qtd_reg_num is not None and saldo_emp_num is not None:
        qtd_emp_num = max(0.0, qtd_reg_num - saldo_emp_num)
    else:
        qtd_emp_num = _to_float(reg.get("quantidadeEmpenhada"))

    return {
        "arquivo_origem":        reg.get("_arquivo_origem", ""),
        "data_extracao":         reg.get("_data_extracao", ""),
        "numero_ata":            reg.get("_numero_ata", ""),
        "numero_item":           reg.get("_numero_item", "") or reg.get("numeroItem", ""),
        "codigo_unidade":        cod_un,
        "nome_unidade":          nome_un,
        "tipo_participacao":     tipo_part,
        "quantidade_registrada": _valor(qtd_reg_num),
        "quantidade_empenhada":  _valor(qtd_emp_num),
        "saldo_empenho":         _valor(saldo_emp_num),
        "data_inclusao":         _data(reg.get("dataHoraInclusao")),
        "data_atualizacao":      _data(reg.get("dataHoraAtualizacao")),
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def transformar(
    pasta_saldos: str = _PASTA_SALDOS,
    caminho_saida: Optional[str] = None,
) -> None:
    global _PASTA_SALDOS
    _PASTA_SALDOS = pasta_saldos

    if caminho_saida is None:
        caminho_saida = os.path.join(
            EXPORT_CONFIG["pasta_saida"], "atas_saldos.csv")

    banco = _indexar()

    if not banco:
        print("⚠️  Nenhum saldo válido encontrado.")
        return

    registros = [_mapear(reg) for reg in banco.values()]
    registros.sort(key=lambda r: (
        r.get("numero_ata") or "",
        r.get("numero_item") or "",
        r.get("codigo_unidade") or "",
    ))

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

    participantes = sum(1 for r in registros if "PARTICIPANTE" in (
        r["tipo_participacao"] or "").upper() and "NÃO" not in (r["tipo_participacao"] or "").upper())
    nao_participantes = sum(1 for r in registros if "NÃO" in (
        r["tipo_participacao"] or "").upper())

    print(f"\n✅ CSV gerado: {caminho_saida}")
    print(f"   Registros        : {len(registros)}")
    print(f"   Participantes    : {participantes}")
    print(f"   Não participantes: {nao_participantes}")
    print(f"   Colunas          : {len(COLUNAS)}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Transforma JSONs de saldos em atas_saldos.csv para Power BI."
    )
    parser.add_argument("--pasta", default=_PASTA_SALDOS)
    parser.add_argument("--saida", default=os.path.join(
        EXPORT_CONFIG["pasta_saida"], "atas_saldos.csv"))
    args = parser.parse_args()
    transformar(pasta_saldos=args.pasta, caminho_saida=args.saida)
