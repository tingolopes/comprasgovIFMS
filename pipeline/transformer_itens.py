"""
pipeline/transformer_itens.py
------------------------------
Varre temp/itens/ e gera itens.csv consolidado para Power BI.

Estratégia de fusão por idCompraItem:
  Prioridade: PNCP > E4 > E6 > E2
  Campos específicos de cada fonte completam os da outra.

Uso como módulo:
    from pipeline.transformer_itens import transformar
    transformar()

Uso via CLI:
    python -m pipeline.transformer_itens
"""

import csv
import glob
import json
import os
import re
import sys
from datetime import datetime
from typing import Optional

from config.config import EXPORT_CONFIG

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
_PASTA_ITENS = "temp/itens"
_SUFIXOS = ["pncp", "E4", "E6", "E2"]   # ordem de prioridade (maior → menor)

# ---------------------------------------------------------------------------
# Schema do CSV final
# ---------------------------------------------------------------------------
COLUNAS = [
    # Rastreabilidade
    "modulo",
    "arquivo_origem",
    "data_extracao",

    # Identificação
    "id_compra",
    "id_compra_item",
    "numero_item",

    # Descrição
    "tipo_material_servico",
    "codigo_item_catalogo",
    "descricao_simples",
    "descricao_detalhada",

    # Quantidades e valores
    "quantidade",
    "unidade_medida",
    "valor_estimado_item",
    "valor_homologado_item",
    "valor_unitario_resultado",
    "valor_total_resultado",
    "menor_lance",
    "valor_negociado",

    # Situação
    "situacao_item",
    "tem_resultado",

    # Fornecedor vencedor
    "fornecedor_vencedor_cnpj",
    "fornecedor_vencedor_nome",

    # Atributos de compra
    "beneficio",
    "criterio_julgamento",
    "decreto7174",
    "sustentavel",

    # Datas
    "data_encerramento",
    "data_adjudicacao",
    "data_homologacao",
    "data_resultado",
    "data_inclusao_pncp",
    "data_atualizacao_pncp",
    "data_alteracao",
]


# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------

def _primeiro(*valores) -> str:
    for v in valores:
        if v is not None and str(v).strip() and str(v).lower() not in ("null", "none", ""):
            return str(v).strip()
    return ""


def _limpar(texto: Optional[str]) -> str:
    if not texto or str(texto).lower() in ("null", "none"):
        return ""
    t = str(texto)
    t = t.replace('"', '')        # remove aspas duplas
    t = t.replace('`', "'")       # substitui crase por aspas simples
    t = t.replace('´', '')        # remove acento agudo solto
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"^- ", "", t)     # remove " - " do início
    return t


def _valor(v) -> str:
    if v is None:
        return ""
    try:
        return f"{float(v):.2f}".replace(".", ",")
    except (ValueError, TypeError):
        return str(v)


# Mapa de normalização de unidade de medida (case-insensitive)
_UNIDADE_MAPA: dict[str, str] = {
    "unidade": "Unidade", "un": "Unidade", "und": "Unidade",
    "und.": "Unidade", "unid": "Unidade", "unid.": "Unidade",
    "quilograma": "Quilograma", "kg": "Quilograma", "kilo": "Quilograma",
    "kilograma": "Quilograma",
    "litro": "Litro", "lt": "Litro", "lts": "Litro", "l": "Litro",
    "metro": "Metro", "mt": "Metro", "m": "Metro",
    "metro quadrado": "Metro Quadrado", "m2": "Metro Quadrado", "m²": "Metro Quadrado",
    "metro cubico": "Metro Cúbico", "m3": "Metro Cúbico", "m³": "Metro Cúbico",
    "caixa": "Caixa", "cx": "Caixa", "cx.": "Caixa",
    "pacote": "Pacote", "pct": "Pacote", "pct.": "Pacote",
    "par": "Par",
    "resma": "Resma",
    "frasco": "Frasco",
    "rolo": "Rolo",
    "conjunto": "Conjunto", "cj": "Conjunto", "cj.": "Conjunto",
    "folha": "Folha",
    "hora": "Hora", "h": "Hora", "hr": "Hora",
    "mes": "Mês", "mês": "Mês",
    "ano": "Ano",
    "servico": "Serviço", "serviço": "Serviço",
    "grupo": "Grupo",
    "embalagem": "Embalagem", "emb": "Embalagem", "emb.": "Embalagem",
}


def _normalizar_unidade(texto: Optional[str]) -> str:
    """Normaliza variações de escrita da unidade de medida."""
    if not texto or str(texto).strip() == "":
        return ""
    chave = str(texto).strip().lower()
    return _UNIDADE_MAPA.get(chave, str(texto).strip().title())


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
    if s in ("true", "1", "sim", "s"):
        return "Sim"
    if s in ("false", "0", "não", "nao", "n"):
        return "Não"
    return str(v)


# ---------------------------------------------------------------------------
# Indexação: agrupa arquivos por idCompra, depois por idCompraItem
# ---------------------------------------------------------------------------

def _sufixo_do_arquivo(nome: str) -> str:
    """Extrai o sufixo (E2, E4, E6, pncp) do nome do arquivo."""
    nome_base = os.path.basename(nome)   # itens_XXXXX_E2_p1.json
    partes = nome_base.replace(".json", "").split("_")
    # Formato: itens_{idCompra}_{sufixo}_p{pagina}
    # O sufixo é o antepenúltimo token
    if len(partes) >= 3:
        return partes[-2]
    return "desconhecido"


def _indexar() -> dict[str, dict[str, dict[str, dict]]]:
    """
    Retorna:
        { idCompra: { idCompraItem: { sufixo: registro, ... } } }
    """
    banco: dict[str, dict[str, dict[str, dict]]] = {}

    jsons = sorted(glob.glob(f"{_PASTA_ITENS}/*.json"))
    jsons += sorted(glob.glob(f"{_PASTA_ITENS}/**/*.json", recursive=True))
    jsons = sorted(set(jsons))

    if not jsons:
        print(f"⚠️  Nenhum JSON em {_PASTA_ITENS}")
        return banco

    print(f"📂 {len(jsons)} arquivo(s) de itens encontrado(s). Processando...")

    for caminho in jsons:
        try:
            with open(caminho, encoding="utf-8") as f:
                envelope = json.load(f)
        except Exception as exc:
            print(f"  ⚠️  Erro ao ler {caminho}: {exc}")
            continue

        if envelope.get("metadata", {}).get("status") != "SUCESSO":
            continue

        sufixo = _sufixo_do_arquivo(caminho)
        arquivo = os.path.basename(caminho)
        data_ext = envelope.get("metadata", {}).get("data_extracao", "")

        respostas = envelope.get("respostas", {})
        resultado = []

        if isinstance(respostas, dict):
            resultado = respostas.get("resultado", []) or []
        elif isinstance(respostas, list):
            resultado = respostas

        for reg in resultado:
            if not isinstance(reg, dict):
                continue

            id_c = str(reg.get("idCompra") or reg.get("id_compra") or "")
            id_item = str(
                reg.get("idCompraItem") or reg.get("id_compra_item") or
                reg.get("nuItemMaterial") or ""
            )

            if not id_c or not id_item:
                continue

            reg["_arquivo_origem"] = arquivo
            reg["_data_extracao"] = data_ext
            reg["_sufixo"] = sufixo

            if id_c not in banco:
                banco[id_c] = {}
            if id_item not in banco[id_c]:
                banco[id_c][id_item] = {}

            banco[id_c][id_item][sufixo] = reg

    return banco


# ---------------------------------------------------------------------------
# Fusão por item
# ---------------------------------------------------------------------------

def _fusionar_item(id_c: str, id_item: str,
                   fontes: dict[str, dict]) -> dict:
    """
    Monta uma linha por idCompraItem priorizando: PNCP > E4 > E6 > E2.
    """
    pncp = fontes.get("pncp", {})
    e4 = fontes.get("E4",   {})
    e6 = fontes.get("E6",   {})
    e2 = fontes.get("E2",   {})

    # Fonte primária para metadados
    master_key = next(
        (k for k in ("pncp", "E4", "E6", "E2") if k in fontes), "E2"
    )
    m = fontes[master_key]

    modulo = "LEI14133" if master_key == "pncp" else "LEGADO"
    arquivo = m.get("_arquivo_origem", "")
    data_ex = m.get("_data_extracao", "")

    # --- Número do item ---
    # E4.coItem é ID interno do SIDEC — não usar como numero_item
    numero_item = _primeiro(
        pncp.get("numeroItemCompra"),
        e2.get("numeroItemLicitacao"),
        e6.get("nuItemMaterial"),
    )

    # --- Tipo e código catálogo ---
    tipo_mat_serv = _primeiro(
        pncp.get("materialOuServicoNome"),
        ("Material" if e2.get("codigoItemMaterial") is not None else "") or
        ("Serviço" if e2.get("codigoItemServico") is not None else ""),
        e6.get("inMaterialServico").replace("material", "Material").replace(
            "servico", "Serviço") if e6.get("inMaterialServico") else "",
        "Grupo"
    )
    codigo_catalogo = _primeiro(
        pncp.get("codItemCatalogo"),
    )

    # --- Descrições ---
    descricao_simples = _limpar(_primeiro(
        e2.get("nomeMaterial") or e2.get("nomeServico"),
        pncp.get("descricaoResumida"),
        e4.get("descricaoItem"),
        e6.get("noServico") or e6.get("noMaterial") or "",
    ))
    descricao_detalhada = _limpar(_primeiro(
        pncp.get("descricaodetalhada"),
        e4.get("descricaoDetalhadaItem"),
        e6.get("dsDetalhada"),
        e2.get("descricaoItem"),
        descricao_simples,             # fallback: usa a simples
    ))

    # --- Quantidades e valores ---
    quantidade = _primeiro(
        pncp.get("quantidade"),
        e2.get("quantidade"),
        e4.get("quantidadeItem"),
        e6.get("qtMaterialAlt"),
    )
    unidade = _normalizar_unidade(_primeiro(
        pncp.get("unidadeMedida"),
        e2.get("unidade"),
        e4.get("unidadeFornecimento"),
        e6.get("noUnidadeMedida"),
        "Grupo"
    ))
    valor_est = _valor(_primeiro(
        pncp.get("valorUnitarioEstimado"),
        e2.get("valorEstimado"),
        e4.get("valorEstimadoItem"),
        e6.get("vrEstimadoItem"),
    ))
    valor_hom = _valor(_primeiro(
        pncp.get("valorUnitarioHomologado"),
        e4.get("valorHomologadoItem"),
    ))
    valor_unit_res = _valor(_primeiro(
        pncp.get("valorUnitarioResultado"),
        e4.get("valorUnitarioResultado"),
    ))
    valor_total_res = _valor(_primeiro(
        pncp.get("valorTotalResultado"),
        e4.get("valorTotalResultado"),
    ))
    menor_lance = _valor(_primeiro(
        e4.get("menorLance"),
    ) if True else _primeiro(e4.get("valorMenorLance")))
    valor_neg = _valor(_primeiro(
        pncp.get("valorNegociado"),
        e4.get("valorNegociado"),
    ))

    # --- Situação ---
    situacao = _primeiro(
        pncp.get("situacaoItemNome"),
        e4.get("situacaoItem"), e4.get("dsSituacaoItem"),
        e2.get("situacaoItem"),
        e6.get("situacaoItem"),
    ).capitalize()
    tem_resultado = _bool_str(_primeiro(
        pncp.get("temResultado"),
        e4.get("temResultado"),
    ))

    # --- Fornecedor vencedor ---
    forn_cnpj = _primeiro(
        pncp.get("codFornecedor"),
        e6.get("nuCnpjVencedor"),
        e4.get("cnpjFornecedor"),
        e2.get("cnpjFornecedor"),
    )
    forn_nome = _limpar(_primeiro(
        pncp.get("nomeFornecedor"),
        e6.get("noFornecedorVencedor"),
        e4.get("nomeFornecedor"),
        e2.get("nomeFornecedor"),
    ))

    # --- Atributos ---
    beneficio = _primeiro(
        pncp.get("tipoBeneficioNome"),
        e2.get("beneficio"), e4.get("beneficio"),
    )
    criterio = _primeiro(
        pncp.get("criterioJulgamentoNome"),
        e4.get("criterioJulgamento"),
    )

    return {
        "modulo":                   modulo,
        "arquivo_origem":           arquivo,
        "data_extracao":            data_ex,
        "id_compra":                id_c,
        "id_compra_item":           id_item,
        "numero_item":              numero_item,
        "tipo_material_servico":    tipo_mat_serv,
        "codigo_item_catalogo":     codigo_catalogo,
        "descricao_simples":        descricao_simples,
        "descricao_detalhada":      descricao_detalhada,
        "quantidade":               quantidade,
        "unidade_medida":           unidade,
        "valor_estimado_item":      valor_est,
        "valor_homologado_item":    valor_hom,
        "valor_unitario_resultado": valor_unit_res,
        "valor_total_resultado":    valor_total_res,
        "menor_lance":              menor_lance,
        "valor_negociado":          valor_neg,
        "situacao_item":            situacao,
        "tem_resultado":            tem_resultado,
        "fornecedor_vencedor_cnpj": forn_cnpj,
        "fornecedor_vencedor_nome": forn_nome,
        "beneficio":                beneficio,
        "criterio_julgamento":      criterio,
        "decreto7174":              _bool_str(_primeiro(
            pncp.get("decreto7174"),
            e4.get("decreto7174"), e2.get("decreto7174"),
        )),
        "sustentavel":              _bool_str(_primeiro(
            pncp.get("sustentavel"),
            e4.get("sustentavel"), e2.get("sustentavel"),
        )),
        "data_encerramento":        _data(_primeiro(
            pncp.get("dataEncerramentoPropostaPncp"),
            e4.get("dtEncerramento"),
        )),
        "data_adjudicacao":         _data(_primeiro(
            pncp.get("dataAdjudicacao"),
            e4.get("dtAdjudic"),
        )),
        "data_homologacao":         _data(_primeiro(
            pncp.get("dataHomologacao"),
            e4.get("dtHom"),
        )),
        "data_resultado":           _data(_primeiro(
            pncp.get("dataResultado"),
            e4.get("dataResultado"),
        )),
        "data_inclusao_pncp":       _data(pncp.get("dataInclusaoPncp")),
        "data_atualizacao_pncp":    _data(pncp.get("dataAtualizacaoPncp")),
        "data_alteracao":           _data(_primeiro(
            pncp.get("dataAlteracao"),
            e6.get("dtAlteracao"),
            e4.get("dtAlteracao"),
            e2.get("dtAlteracao"),
        )),
    }


# ---------------------------------------------------------------------------
# Ponto de entrada público
# ---------------------------------------------------------------------------

def transformar(
    pasta_itens: str = _PASTA_ITENS,
    caminho_saida: Optional[str] = None,
) -> None:
    global _PASTA_ITENS
    _PASTA_ITENS = pasta_itens

    if caminho_saida is None:
        caminho_saida = os.path.join(EXPORT_CONFIG["pasta_saida"], "itens.csv")

    banco = _indexar()

    registros = []
    for id_c, itens in banco.items():
        for id_item, fontes in itens.items():
            registros.append(_fusionar_item(id_c, id_item, fontes))

    if not registros:
        print("⚠️  Nenhum item válido encontrado.")
        sys.exit(1)

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

    total_leg = sum(1 for r in registros if r["modulo"] == "LEGADO")
    total_pncp = sum(1 for r in registros if r["modulo"] == "LEI14133")

    print(f"\n✅ CSV gerado: {caminho_saida}")
    print(f"   Itens únicos  : {len(registros)}")
    print(f"   Legado        : {total_leg}")
    print(f"   Lei 14.133    : {total_pncp}")
    print(f"   Colunas       : {len(COLUNAS)}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Transforma JSONs de itens em itens.csv para Power BI."
    )
    parser.add_argument("--pasta", default=_PASTA_ITENS,
                        help="Pasta com os JSONs de itens")
    parser.add_argument(
        "--saida",
        default=os.path.join(EXPORT_CONFIG["pasta_saida"], "itens.csv"),
        help="Caminho do CSV de saída",
    )
    args = parser.parse_args()
    transformar(pasta_itens=args.pasta, caminho_saida=args.saida)
