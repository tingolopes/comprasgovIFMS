"""
pipeline/transformer_atas_unidades.py
---------------------------------------
Varre temp/atas_unidades/ e gera atas_unidades.csv para Power BI.

Cada linha representa: (numeroAta × numeroItem × codigoUnidade).
Deduplicação por (numero_ata + numero_item + codigo_unidade) —
mantém o registro com data_extracao mais recente.

O campo 'fornecedor' vem no formato "CNPJ - RAZAO SOCIAL" e é separado
em fornecedor_cnpj e fornecedor_nome para facilitar filtragem no Power BI.

Uso como módulo:
    from pipeline.transformer_atas_unidades import transformar
    transformar()

Uso via CLI:
    python -m pipeline.transformer_atas_unidades
"""

import csv
import glob
import json
import os
import re
import sys
from datetime import datetime
from typing import Optional

from config.config import EXPORT_CONFIG, CONFIG_ATAS, CONFIG_APIS, UASGS

_PASTA_UNIDADES = CONFIG_ATAS["pasta_cache_unidades"]
_PASTA_SALDOS = CONFIG_ATAS["pasta_cache_saldos"]
_PASTA_ITENS = CONFIG_ATAS["pasta_cache_itens"]

_SIGLA_POR_UASG = {u['codigo']: u['sigla'] for u in UASGS}

# ---------------------------------------------------------------------------
# Schema do CSV final
# ---------------------------------------------------------------------------
COLUNAS = [
    # Rastreabilidade
    "arquivo_origem",
    "data_extracao",

    # Identificação da ata e item
    "numero_ata",
    "uasg_gerenciadora",
    "numero_item",
    "id_compra",
    "id_compra_item",
    "codigo_pdm",
    "descricao_item",
    "tipo_item",
    "valor_unitario",

    # Fornecedor vencedor
    "fornecedor_cnpj",
    "fornecedor_nome",

    # Unidade participante
    "codigo_unidade",
    "nome_unidade",
    "tipo_unidade",
    "sigla_unidade",
    "aceita_adesao",

    # Quantidades e saldos
    "quantidade_registrada",
    "saldo_adesoes",
    "saldo_remanejamento_empenho",
    "qtd_limite_adesao",
    "qtd_limite_informado_compra",
    "quantidade_empenhada",

    # Datas
    "data_inclusao",
    "data_atualizacao",
    "data_exclusao",
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
    # Permite vírgula decimal
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


def _parse_fornecedor(fornecedor_str: str) -> tuple[str, str]:
    """
    Extrai (cnpj, nome) do campo fornecedor.
    Formato: "26722189000110 - CERRADO VIAGENS LTDA"
    """
    if not fornecedor_str:
        return "", ""
    partes = str(fornecedor_str).split(" - ", 1)
    cnpj = partes[0].strip()
    nome = _limpar(partes[1]) if len(partes) > 1 else ""
    return cnpj, nome


def _parse_unidade(unidade_str: str) -> str:
    """Extrai código da unidade de "codigo - nome"."""
    if not unidade_str:
        return ""
    return str(unidade_str).split(" - ", 1)[0].strip()


def _indexar_saldos() -> dict[str, str]:
    """Retorna mapa chave->quantidadeEmpenhada para saldos deduplicados."""
    mapa: dict[str, str] = {}
    jsons = sorted(glob.glob(f"{_PASTA_SALDOS}/*.json"))
    for caminho in jsons:
        try:
            with open(caminho, encoding="utf-8") as f:
                envelope = json.load(f)
        except Exception:
            continue
        if envelope.get("metadata", {}).get("status") != "SUCESSO":
            continue
        numero_ata = ""
        arquivo = os.path.basename(caminho)
        # Tenta extrair numero_ata do nome (como transformer_atas_saldos)
        if arquivo.startswith("atas_saldos_RT_"):
            partes = arquivo.replace(".json", "").split("_")
            if len(partes) >= 5:
                numero_ata = f"{partes[-3]}/{partes[-2]}"
        respostas = envelope.get("respostas", {})
        resultado = (respostas.get("resultado", []) or []
                     ) if isinstance(respostas, dict) else []
        for reg in resultado:
            if not isinstance(reg, dict):
                continue
            num_item = str(reg.get("numeroItem") or "")
            cod_un = _parse_unidade(reg.get("unidade", ""))
            if not numero_ata or not num_item or not cod_un:
                continue
            chave = f"{numero_ata}|{num_item}|{cod_un}"
            quantidade_empenhada = _valor(reg.get("quantidadeEmpenhada"))
            if quantidade_empenhada:
                mapa[chave] = quantidade_empenhada
    return mapa


def _indexar_itens_info() -> dict[str, dict[str, str]]:
    """
    Retorna mapa { "numero_ata|numero_item": { tipo_item, valor_unitario,
                                               id_compra, id_compra_item } }

    idCompra e obtido diretamente do atas_itens quando disponivel.
    Fallback: quando idCompra esta vazio, busca nos JSONs de temp/compras/
    via numeroControlePncpCompra -> numeroControlePNCP.
    """

    # ------------------------------------------------------------------
    # Pre-carrega mapa de fallback: numeroControlePncpCompra -> idCompra
    # lendo todos os JSONs pncp_* de temp/compras/
    # ------------------------------------------------------------------
    pasta_compras = CONFIG_APIS["LEI14133"]["pasta_cache"]
    mapa_ctrl_para_id: dict[str, str] = {}

    for caminho in sorted(glob.glob(os.path.join(pasta_compras, "pncp_*.json"))):
        try:
            with open(caminho, encoding="utf-8") as f:
                envelope = json.load(f)
        except Exception:
            continue
        if envelope.get("metadata", {}).get("status") != "SUCESSO":
            continue
        respostas = envelope.get("respostas", {})
        resultado = (respostas.get("resultado", []) or []
                     ) if isinstance(respostas, dict) else []
        for compra in resultado:
            if not isinstance(compra, dict):
                continue
            ctrl = str(compra.get("numeroControlePNCP") or "").strip()
            id_c = str(compra.get("idCompra") or "").strip()
            if ctrl and id_c:
                mapa_ctrl_para_id[ctrl] = id_c

    # ------------------------------------------------------------------
    # Le atas_itens e monta o mapa principal
    # ------------------------------------------------------------------
    mapa: dict[str, dict[str, str]] = {}
    jsons = sorted(glob.glob(f"{_PASTA_ITENS}/*.json"))

    for caminho in jsons:
        try:
            with open(caminho, encoding="utf-8") as f:
                envelope = json.load(f)
        except Exception:
            continue
        if envelope.get("metadata", {}).get("status") != "SUCESSO":
            continue
        respostas = envelope.get("respostas", {})
        resultado = (respostas.get("resultado", []) or []
                     ) if isinstance(respostas, dict) else []
        for reg in resultado:
            if not isinstance(reg, dict):
                continue
            num_ata  = str(reg.get("numeroAtaRegistroPreco")
                           or reg.get("numeroAta") or "")
            num_item = str(reg.get("numeroItem") or "").strip().zfill(5)
            if not num_ata or not num_item:
                continue

            chave = f"{num_ata}|{num_item}"

            # idCompra: direto do registro ou via fallback pelo ctrl compra
            id_compra = str(reg.get("idCompra") or "").strip()
            if not id_compra:
                ctrl_compra = str(reg.get("numeroControlePncpCompra") or "").strip()
                id_compra   = mapa_ctrl_para_id.get(ctrl_compra, "")

            id_compra_item = f"{id_compra}{num_item}" if id_compra else ""

            tipo_item      = str(reg.get("tipoItem") or "")
            valor_unitario = _valor(reg.get("valorUnitario"))

            if chave not in mapa:
                mapa[chave] = {}
            if tipo_item:
                mapa[chave]["tipo_item"]      = tipo_item
            if valor_unitario:
                mapa[chave]["valor_unitario"] = valor_unitario
            if id_compra:
                mapa[chave]["id_compra"]      = id_compra
            if id_compra_item:
                mapa[chave]["id_compra_item"] = id_compra_item

    return mapa


# ---------------------------------------------------------------------------
# Indexação — deduplicação por (numero_ata + numero_item + codigo_unidade)
# ---------------------------------------------------------------------------

def _indexar() -> dict[str, dict]:
    """
    Retorna { "numero_ata|numero_item|codigo_unidade": registro } — deduplicado.
    Em caso de duplicata mantém o registro com data_extracao mais recente.
    """
    banco: dict[str, dict] = {}

    jsons = sorted(glob.glob(f"{_PASTA_UNIDADES}/*.json"))
    if not jsons:
        print(f"⚠️  Nenhum JSON em {_PASTA_UNIDADES}")
        return banco

    print(f"📂 {len(jsons)} arquivo(s) de unidades encontrado(s). Processando...")

    saldos = _indexar_saldos()
    itens_info = _indexar_itens_info()

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
        resultado = (
            respostas.get("resultado", []) or []
            if isinstance(respostas, dict) else []
        )

        for reg in resultado:
            if not isinstance(reg, dict):
                continue

            num_ata = str(reg.get("numeroAta") or "")
            num_item = str(reg.get("numeroItem") or "")
            cod_un = str(reg.get("codigoUnidade") or "")

            if not num_ata or not num_item or not cod_un:
                continue

            chave = f"{num_ata}|{num_item}|{cod_un}"
            reg["_arquivo_origem"] = arquivo
            reg["_data_extracao"] = data_ext
            reg["_quantidade_empenhada"] = saldos.get(chave, "")
            reg["_sigla_unidade"] = _SIGLA_POR_UASG.get(
                str(reg.get("codigoUnidade") or ""), "")
            info = itens_info.get(f"{num_ata}|{num_item}", {})
            reg["_tipo_item"]      = info.get("tipo_item", "")
            reg["_valor_unitario"] = info.get("valor_unitario", "")
            reg["_id_compra"]      = info.get("id_compra", "")
            reg["_id_compra_item"] = info.get("id_compra_item", "")

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
    cnpj, nome_forn = _parse_fornecedor(reg.get("fornecedor", ""))
    reg_map = {
        "arquivo_origem":               reg.get("_arquivo_origem", ""),
        "data_extracao":                reg.get("_data_extracao", ""),
        "numero_ata":                   reg.get("numeroAta", ""),
        "uasg_gerenciadora":            reg.get("unidadeGerenciadora", ""),
        "numero_item":                  reg.get("numeroItem", ""),
        "id_compra":                    reg.get("_id_compra", ""),
        "id_compra_item":               reg.get("_id_compra_item", ""),
        "codigo_pdm":                   reg.get("codigoPdm", "") or "",
        "descricao_item":               _limpar(reg.get("descricaoItem")),
        "tipo_item":                    reg.get("_tipo_item", ""),
        "valor_unitario":               reg.get("_valor_unitario", ""),
        "fornecedor_cnpj":              cnpj,
        "fornecedor_nome":              nome_forn,
        "codigo_unidade":               reg.get("codigoUnidade", ""),
        "nome_unidade":                 _limpar(reg.get("nomeUnidade")),
        "tipo_unidade":                 reg.get("tipoUnidade", ""),
        "sigla_unidade":                reg.get("_sigla_unidade", ""),
        "aceita_adesao":                _bool_str(reg.get("aceitaAdesao")),
        "quantidade_registrada":        _valor(reg.get("quantidadeRegistrada")),
        "saldo_adesoes":                _valor(reg.get("saldoAdesoes")),
        "saldo_remanejamento_empenho":  _valor(reg.get("saldoRemanejamentoEmpenho")),
        "qtd_limite_adesao":            _valor(reg.get("qtdLimiteAdesao")),
        "qtd_limite_informado_compra":  _valor(reg.get("qtdLimiteInformadoCompra")),
    }

    qtd_registrada_num = _to_float(reg.get("quantidadeRegistrada"))
    qtd_empenhada_num = _to_float(reg.get("_quantidade_empenhada"))
    if qtd_registrada_num is not None and qtd_empenhada_num is not None:
        if qtd_empenhada_num > qtd_registrada_num:
            reg_map["quantidade_empenhada"] = _valor(qtd_registrada_num)
        else:
            reg_map["quantidade_empenhada"] = _valor(qtd_empenhada_num)
    else:
        reg_map["quantidade_empenhada"] = _valor(
            reg.get("_quantidade_empenhada"))

    reg_map.update({
        "data_inclusao":                _data(reg.get("dataHoraInclusao")),
        "data_atualizacao":             _data(reg.get("dataHoraAtualizacao")),
        "data_exclusao":                _data(reg.get("dataHoraExclusao")),
    })
    return reg_map


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def transformar(
    pasta_unidades: str = _PASTA_UNIDADES,
    caminho_saida: Optional[str] = None,
) -> None:
    global _PASTA_UNIDADES
    _PASTA_UNIDADES = pasta_unidades

    if caminho_saida is None:
        caminho_saida = os.path.join(
            EXPORT_CONFIG["pasta_saida"], "atas_unidades.csv")

    banco = _indexar()

    if not banco:
        print("⚠️  Nenhuma unidade válida encontrada.")
        sys.exit(1)

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

    participantes = sum(
        1 for r in registros if r["tipo_unidade"] == "PARTICIPANTE")
    nao_participantes = sum(
        1 for r in registros if r["tipo_unidade"] != "PARTICIPANTE" and r["tipo_unidade"])
    aceita_adesao = sum(1 for r in registros if r["aceita_adesao"] == "Sim")

    print(f"\n✅ CSV gerado: {caminho_saida}")
    print(f"   Registros        : {len(registros)}")
    print(f"   Participantes    : {participantes}")
    print(f"   Não participantes: {nao_participantes}")
    print(f"   Aceita adesão    : {aceita_adesao}")
    print(f"   Colunas          : {len(COLUNAS)}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Transforma JSONs de unidades em atas_unidades.csv para Power BI."
    )
    parser.add_argument("--pasta", default=_PASTA_UNIDADES)
    parser.add_argument("--saida", default=os.path.join(
        EXPORT_CONFIG["pasta_saida"], "atas_unidades.csv"))
    args = parser.parse_args()
    transformar(pasta_unidades=args.pasta, caminho_saida=args.saida)