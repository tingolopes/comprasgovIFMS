"""
pipeline/transformer_compras.py
--------------------------------
Varre todos os JSONs de cache de compras e gera um CSV normalizado.

Estratégia de fusão:
  O mesmo idCompra pode aparecer em múltiplos arquivos/endpoints.
  Agrupa por idCompra e monta uma linha única priorizando a fonte mais
  rica para cada campo (PNCP > LEG_E3 > LEG_E5 > LEG_E1).

Uso como módulo:
    from pipeline.transformer_compras import transformar
    transformar(pastas=["temp/compras"], caminho_saida="data/compras.csv")

Uso via CLI:
    python -m pipeline.transformer_compras
    python -m pipeline.transformer_compras --pasta temp/compras --saida data/compras.csv
"""

import csv
import glob
import json
import os
import re
import sys
from datetime import datetime
from typing import Optional

from config.config import CONFIG_APIS, EXPORT_CONFIG, MODALIDADES
from pipeline.logger import log_aviso, log_info

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
    "numero_processo",
    "numero_compra",
    "numero_controle_pncp",

    # Unidade Gestora
    "uasg_codigo",
    "uasg_nome",
    "uasg_sigla",

    # Modalidade / situação
    "modalidade_codigo",
    "modalidade_nome",
    "lei_14133",
    "amparo_legal",
    "modo_disputa",
    "situacao",

    # Objeto
    "objeto",

    # Responsáveis (exclusivo Legado / dispensa)
    "responsavel_declaracao",
    "cargo_declaracao",
    "responsavel_ratificacao",
    "cargo_ratificacao",

    # Valores
    "valor_estimado",
    "valor_homologado",

    # Datas
    "data_publicacao",
    "data_abertura_proposta",
    "data_encerramento_proposta",
    "data_alteracao",
]

# ---------------------------------------------------------------------------
# Tabelas de referência
# ---------------------------------------------------------------------------
_SIGLA_POR_UASG: dict[str, str] = {
    u["codigo"]: u["sigla"] for u in CONFIG_APIS["LEGADO"]["uasgs"]
}
_NOME_POR_UASG: dict[str, str] = {
    u["codigo"]: u.get("nome", "") for u in CONFIG_APIS["LEGADO"]["uasgs"]
}

# Mapeamento prefixo-de-arquivo → código de fonte
_FONTE_POR_PREFIXO: dict[str, str] = {
    "pncp_":              "PNCP",
    "pregao_":            "LEG_E3",
    "dispensa_":          "LEG_E5",
    "outrasmodalidades_": "LEG_E1",
}


# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------

def _fonte(arquivo: str) -> str:
    nome = os.path.basename(arquivo).lower()
    for prefixo, fonte in _FONTE_POR_PREFIXO.items():
        if nome.startswith(prefixo):
            return fonte
    return "OUTRO"


def _limpar(texto: Optional[str]) -> str:
    if not texto or str(texto).lower() in ("null", "none"):
        return ""
    t = str(texto)
    t = t.replace('"', '')        # remove aspas duplas
    t = t.replace('`', "'")       # substitui crase por aspas simples
    t = t.replace('´', '')        # remove acento agudo solto
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"^- ", "", t)     # remove " - " do início
    for prefixo in ("Objeto:", "Fundamento Legal:", "Justificativa:"):
        if t.startswith(prefixo):
            t = t[len(prefixo):].strip()
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


def _formatar_processo(num: str) -> str:
    limpo = "".join(filter(str.isdigit, str(num)))
    if limpo.startswith("23347") and len(limpo) == 17:
        return f"{limpo[:5]}.{limpo[5:11]}.{limpo[11:15]}-{limpo[15:]}"
    return num


def _primeiro(*valores) -> str:
    for v in valores:
        if v and str(v).strip() and str(v).lower() != "null":
            return str(v).strip()
    return ""


# ---------------------------------------------------------------------------
# Parse do id_compra
# Estrutura: UASG(6) + MODALIDADE(2) + NUMERO(5) + ANO(4) = 17 dígitos
# Exemplo:   158450    06              00021         2016
# ---------------------------------------------------------------------------

def _parse_id_compra(id_compra: str) -> dict[str, str]:
    digitos = "".join(filter(str.isdigit, str(id_compra)))
    if len(digitos) != 17:
        return {}

    uasg = digitos[0:6]
    mod = digitos[6:8]
    numero = digitos[8:13]
    ano = digitos[13:17]

    try:
        cod_mod_int = int(mod)
    except ValueError:
        cod_mod_int = None

    return {
        "uasg_codigo":       uasg,
        "modalidade_codigo": str(cod_mod_int) if cod_mod_int is not None else mod,
        "modalidade_nome":   MODALIDADES.get(cod_mod_int, ""),
        "numero_compra":     f"{numero}/{ano}",
        "ano_compra":        ano,
    }


# ---------------------------------------------------------------------------
# Motor de coleta: agrupa registros por idCompra × fonte
# ---------------------------------------------------------------------------

def _coletar_por_id(pastas: list[str]) -> dict[str, dict[str, dict]]:
    """
    Retorna { idCompra: { "PNCP": {...}, "LEG_E3": {...}, ... } }
    Mantém o registro mais recente quando o mesmo idCompra aparece em
    múltiplos arquivos da mesma fonte.
    """
    banco: dict[str, dict[str, dict]] = {}

    todos_jsons: list[str] = []
    for pasta in pastas:
        todos_jsons += glob.glob(f"{pasta}/*.json")
        todos_jsons += glob.glob(f"{pasta}/**/*.json", recursive=True)

    todos_jsons = sorted(set(todos_jsons))

    if not todos_jsons:
        log_aviso("Nenhum arquivo JSON encontrado nas pastas: %s", pastas)
        sys.exit(1)

    log_info("📂 %d arquivo(s) encontrado(s). Processando...", len(todos_jsons))

    for caminho in todos_jsons:
        try:
            with open(caminho, encoding="utf-8") as f:
                envelope = json.load(f)
        except Exception as exc:
            log_aviso("Erro ao ler %s: %s", caminho, exc)
            continue

        if envelope.get("metadata", {}).get("status") != "SUCESSO":
            continue

        resultado = envelope.get("respostas", {}).get("resultado") or []
        if not isinstance(resultado, list):
            continue

        fonte = _fonte(caminho)
        arquivo = os.path.basename(caminho)
        data_ext = envelope.get("metadata", {}).get("data_extracao", "")

        for reg in resultado:
            if not isinstance(reg, dict):
                continue
            id_c = reg.get("idCompra") or reg.get("id_compra")
            if not id_c:
                continue

            reg["_arquivo_origem"] = arquivo
            reg["_data_extracao"] = data_ext

            if id_c not in banco:
                banco[id_c] = {}

            # Mantém o mais recente para cada fonte
            if fonte not in banco[id_c] or \
               data_ext >= banco[id_c][fonte].get("_data_extracao", ""):
                banco[id_c][fonte] = reg

    return banco


# ---------------------------------------------------------------------------
# Motor de fusão: monta uma linha por idCompra
# ---------------------------------------------------------------------------

def _fusionar(id_c: str, fontes: dict[str, dict]) -> dict:
    """
    Prioridade geral: PNCP > LEG_E3 > LEG_E5 > LEG_E1
    Campos extraídos do id_compra são usados como fallback confiável.
    """
    pncp = fontes.get("PNCP",    {})
    e3 = fontes.get("LEG_E3",  {})
    e5 = fontes.get("LEG_E5",  {})
    e1 = fontes.get("LEG_E1",  {})

    master_key = next(
        (k for k in ("PNCP", "LEG_E3", "LEG_E5", "LEG_E1") if k in fontes),
        "OUTRO",
    )
    m = fontes[master_key]

    parsed = _parse_id_compra(id_c)

    # --- Metadados ---
    modulo = "LEI14133" if master_key == "PNCP" else "LEGADO"
    arquivo = m.get("_arquivo_origem", "")
    data_ext = m.get("_data_extracao", "")

    # --- UASG ---
    uasg_codigo = _primeiro(
        pncp.get("unidadeOrgaoCodigoUnidade"),
        e5.get("co_uasg"), e3.get("co_uasg"), e3.get("uasg"),
        e1.get("uasg"), m.get("co_uasg"), m.get("uasg"),
        parsed.get("uasg_codigo"),
    )
    uasg_nome = _NOME_POR_UASG.get(uasg_codigo) or _primeiro(
        pncp.get("unidadeOrgaoNomeUnidade"),
        e5.get("no_ausg"), e3.get("no_uasg"),
    )

    # --- Processo ---
    num_processo = _primeiro(
        pncp.get("processo"),
        e3.get("nu_processo"), e5.get("nu_processo"),
        e1.get("numero_processo"), m.get("co_processo"),
    )

    # --- Número da compra ---
    numero_compra = _primeiro(
        parsed.get("numero_compra"),
        pncp.get("numeroCompra"),
        e3.get("nu_pregao_original"), e3.get("nu_aviso_licitacao"),
        e5.get("nu_aviso_licitacao"), e1.get("numero_aviso"),
    )

    # --- Modalidade ---
    cod_mod = _primeiro(
        pncp.get("codigoModalidade"),
        m.get("co_modalidade_licitacao"), e1.get("modalidade"),
        parsed.get("modalidade_codigo"),
    )
    try:
        cod_mod_int = int(cod_mod)
    except (ValueError, TypeError):
        cod_mod_int = None

    nome_mod = _primeiro(
        MODALIDADES.get(cod_mod_int, ""),
        pncp.get("modalidadeNome"),
        e1.get("nome_modalidade"),
        parsed.get("modalidade_nome"),
    )

    # --- Lei 14.133 ---
    lei_14133 = "Sim" if modulo == "LEI14133" else "Não"

    # --- Objeto ---
    objeto = _limpar(_primeiro(
        pncp.get("objetoCompra"),
        e1.get("objeto"),
        e5.get("ds_objeto_licitacao"),
        e3.get("ds_objeto"), e3.get("ds_objeto_licitacao"),
        e3.get("objeto"), e3.get("no_objeto"),
        m.get("tx_objeto"), m.get("ds_justificativa"),
    ))

    # --- Responsáveis (exclusivo Legado) ---
    responsavel_declaracao = _limpar(_primeiro(
        e5.get("no_responsavel_decl_disp"),
        e1.get("no_responsavel_decl_disp"), e1.get("nome_responsavel"),
        m.get("no_responsavel_decl_disp"),
    ))
    cargo_declaracao = _limpar(_primeiro(
        e5.get("no_cargo_resp_decl_disp"),
        e1.get("no_cargo_resp_decl_disp"), e1.get("funcao_responsavel"),
        m.get("no_cargo_resp_decl_disp"),
    ))
    responsavel_ratificacao = _limpar(_primeiro(
        e5.get("no_responsavel_ratificacao"),
        e1.get("no_responsavel_ratificacao"),
        m.get("no_responsavel_ratificacao"),
    ))
    cargo_ratificacao = _limpar(_primeiro(
        e5.get("no_cargo_resp_ratificacao"),
        e1.get("no_cargo_resp_ratificacao"),
        m.get("no_cargo_resp_ratificacao"),
    ))

    # --- Valores ---
    valor_estimado = _valor(_primeiro(
        pncp.get("valorTotalEstimado"),
        e1.get("valor_estimado_total"),
        e5.get("vr_estimado"), e3.get(
            "vr_estimado"), e3.get("vr_estimado_total"),
    ))
    valor_homologado = _valor(_primeiro(
        pncp.get("valorTotalHomologado"),
        e1.get("valor_homologado_total"),
        e3.get("vr_homologado"), e3.get("vr_homologado_total"),
    ))

    # --- Datas ---
    data_publicacao = _data(_primeiro(
        pncp.get("dataPublicacaoPncp"),
        e1.get("data_publicacao"), e3.get("dt_publicacao"),
        e5.get("dtPublicacao"), e3.get("data_publicacao"),
    ))
    data_abertura = _data(_primeiro(
        pncp.get("dataAberturaPropostaPncp"),
        e1.get("data_abertura_proposta"), e3.get("dt_abertura"),
        e3.get("data_abertura_proposta"),
    ))
    data_encerramento = _data(_primeiro(
        pncp.get("dataEncerramentoPropostaPncp"),
        e1.get("data_entrega_proposta"), e3.get("dt_entrega_proposta"),
        e3.get("data_entrega_proposta"),
    ))
    data_alteracao = _data(_primeiro(
        pncp.get("dataAtualizacaoPncp"),
        m.get("dt_alteracao"),
    ))

    return {
        "modulo":                     modulo,
        "arquivo_origem":             arquivo,
        "data_extracao":              data_ext,
        "id_compra":                  id_c,
        "numero_processo":            _formatar_processo(num_processo),
        "numero_compra":              numero_compra,
        "numero_controle_pncp":       pncp.get("numeroControlePNCP", ""),
        "uasg_codigo":                uasg_codigo,
        "uasg_nome":                  uasg_nome,
        "uasg_sigla":                 _SIGLA_POR_UASG.get(uasg_codigo, ""),
        "modalidade_codigo":          cod_mod,
        "modalidade_nome":            nome_mod,
        "lei_14133":                  lei_14133,
        "amparo_legal":               _limpar(_primeiro(
            pncp.get("amparoLegalNome"),
            e5.get("ds_fundamento_legal"), e3.get("ds_fundamento_legal"),
        )),
        "modo_disputa":               _primeiro(
            pncp.get("modoDisputaNomePncp"),
            e1.get("tipo_pregao"), e3.get("tipo_pregao"),
        ).capitalize(),
        "situacao":                   _primeiro(
            pncp.get("situacaoCompraNomePncp"),
            e3.get("ds_situacao_pregao"), e1.get("situacao_aviso"),
        ).capitalize(),
        "objeto":                     objeto,
        "responsavel_declaracao":     responsavel_declaracao,
        "cargo_declaracao":           cargo_declaracao,
        "responsavel_ratificacao":    responsavel_ratificacao,
        "cargo_ratificacao":          cargo_ratificacao,
        "valor_estimado":             valor_estimado,
        "valor_homologado":           valor_homologado,
        "data_publicacao":            data_publicacao,
        "data_abertura_proposta":     data_abertura,
        "data_encerramento_proposta": data_encerramento,
        "data_alteracao":             data_alteracao,
    }


# ---------------------------------------------------------------------------
# Ponto de entrada público
# ---------------------------------------------------------------------------

def transformar(pastas: list[str], caminho_saida: str) -> None:
    banco = _coletar_por_id(pastas)
    registros = [_fusionar(id_c, fontes) for id_c, fontes in banco.items()]

    if not registros:
        log_aviso("Nenhum registro válido encontrado.")
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

    total_legado = sum(1 for r in registros if r["modulo"] == "LEGADO")
    total_14133 = sum(1 for r in registros if r["modulo"] == "LEI14133")
    sem_objeto = sum(1 for r in registros if not r["objeto"])
    com_resp = sum(1 for r in registros if r["responsavel_declaracao"])

    log_info("✅ CSV gerado: %s", caminho_saida)
    log_info("   IDs únicos     : %d", len(registros))
    log_info("   Legado         : %d", total_legado)
    log_info("   Lei 14.133     : %d", total_14133)
    log_info("   Com responsável: %d", com_resp)
    log_info("   Sem objeto     : %d", sem_objeto)
    log_info("   Colunas        : %d", len(COLUNAS))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args():
    import argparse
    parser = argparse.ArgumentParser(
        description="Transforma JSONs de cache em compras.csv para Power BI."
    )
    parser.add_argument(
        "--pasta", nargs="+",
        default=[CONFIG_APIS["LEGADO"]["pasta_cache"]],
        help="Pasta(s) com os JSONs (padrão: config.py)",
    )
    parser.add_argument(
        "--saida",
        default=os.path.join(EXPORT_CONFIG["pasta_saida"], "compras.csv"),
        help="Caminho do CSV de saída",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    transformar(pastas=args.pasta, caminho_saida=args.saida)
