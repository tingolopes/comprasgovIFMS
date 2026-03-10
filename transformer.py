"""
transformer.py
--------------
Varre todos os arquivos JSON de cache e gera um único CSV normalizado,
com colunas padronizadas independente do módulo de origem (Legado ou 14133).

Estratégia de fusão:
  O mesmo idCompra pode aparecer em múltiplos arquivos/endpoints
  (ex: uma dispensa consta em LEG_E5 e também no PNCP). Ao invés de
  processar arquivo por arquivo, primeiro agrupa todos os registros
  pelo idCompra e depois monta uma linha única com a melhor informação
  disponível em cada fonte — exatamente como o script de referência faz.

Uso:
    python transformer.py                         # usa pasta padrão do config.py
    python transformer.py --pasta temp/meus_jsons # pasta customizada
    python transformer.py --saida relatorio.csv   # nome do arquivo de saída
"""

import argparse
import csv
import glob
import json
import os
import re
import sys
from datetime import datetime
from typing import Optional

from config import CONFIG_APIS, EXPORT_CONFIG, MODALIDADES

# ---------------------------------------------------------------------------
# Schema do CSV final — ordem e nome das colunas
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

    # Modalidade
    "modalidade_codigo",
    "modalidade_nome",
    "amparo_legal",
    "modo_disputa",
    "situacao",

    # Objeto
    "objeto",

    # Responsáveis (disponíveis apenas no módulo Legado / dispensa)
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
# Tabelas de referência (construídas a partir do config.py)
# ---------------------------------------------------------------------------
_SIGLA_POR_UASG: dict[str, str] = {
    u["codigo"]: u["sigla"] for u in CONFIG_APIS["LEGADO"]["uasgs"]
}

_NOME_POR_UASG: dict[str, str] = {
    u["codigo"]: u.get("nome", "") for u in CONFIG_APIS["LEGADO"]["uasgs"]
}

# Identificador de fonte pelo prefixo do nome do arquivo
_FONTE_POR_PREFIXO: dict[str, str] = {
    "pncp_":              "PNCP",
    "pregao_":            "LEG_E3",
    "dispensa_":          "LEG_E5",
    "outrasmodalidades_": "LEG_E1",
}


# ---------------------------------------------------------------------------
# Parse do id_compra
# Estrutura: UASG(6) + MODALIDADE(2) + NUMERO(6) + ANO(4) = 18 dígitos
# Exemplo:   158450    06              000021       2016
# ---------------------------------------------------------------------------

def _parse_id_compra(id_compra: str) -> dict[str, str]:
    """
    Extrai os campos embutidos no id_compra.

    '15845006000212016' →
        uasg_codigo  = '158450'
        modalidade   = '06'  →  6  →  'Dispensa'
        numero       = '000021'
        ano          = '2016'
        numero_compra = '000021/2016'
    """
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
        "uasg_codigo":      uasg,
        "modalidade_codigo": str(cod_mod_int) if cod_mod_int is not None else mod,
        "modalidade_nome":  MODALIDADES.get(cod_mod_int, ""),
        "numero_compra":    f"{numero}/{ano}",
        "ano_compra":       ano,
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
    """Remove espaços extras, quebras de linha e prefixos redundantes."""
    if not texto or str(texto).lower() == "null":
        return ""
    texto = re.sub(r"\s+", " ", str(texto)).strip()
    for prefixo in ("Objeto:", "Fundamento Legal:", "Justificativa:"):
        if texto.startswith(prefixo):
            texto = texto[len(prefixo):].strip()
    return texto


def _valor(v) -> str:
    """Formata número para string com 2 casas decimais no padrão pt-BR."""
    if v is None:
        return ""
    try:
        return f"{float(v):.2f}".replace(".", ",")
    except (ValueError, TypeError):
        return str(v)


def _data(valor: Optional[str]) -> str:
    """Normaliza datas/datetimes para DD/MM/YYYY."""
    if not valor:
        return ""
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(valor)[:19], fmt).strftime("%d/%m/%Y")
        except (ValueError, TypeError):
            continue
    return str(valor)


def _formatar_processo(num_processo: str) -> str:
    """Formata número de processo no padrão IFMS: 23347.000000.0000-00"""
    limpo = "".join(filter(str.isdigit, str(num_processo)))
    if limpo.startswith("23347") and len(limpo) == 17:
        return f"{limpo[:5]}.{limpo[5:11]}.{limpo[11:15]}-{limpo[15:]}"
    return num_processo


def _primeiro(*valores) -> str:
    """Retorna o primeiro valor não-vazio da lista."""
    for v in valores:
        if v and str(v).strip() and str(v).lower() != "null":
            return str(v).strip()
    return ""


# ---------------------------------------------------------------------------
# Motor de coleta: agrupa registros por idCompra × fonte
# ---------------------------------------------------------------------------

def _coletar_por_id(pastas: list[str]) -> dict[str, dict[str, dict]]:
    """
    Varre os JSONs e retorna:
        { idCompra: { "PNCP": {...}, "LEG_E5": {...}, ... } }

    Se o mesmo idCompra aparecer em mais de um arquivo da mesma fonte,
    mantém o registro mais recente (maior data_extracao).
    """
    banco: dict[str, dict[str, dict]] = {}

    todos_jsons: list[str] = []
    for pasta in pastas:
        todos_jsons += glob.glob(f"{pasta}/*.json")
        todos_jsons += glob.glob(f"{pasta}/**/*.json", recursive=True)

    todos_jsons = sorted(set(todos_jsons))

    if not todos_jsons:
        print(f"⚠️  Nenhum arquivo JSON encontrado nas pastas: {pastas}")
        sys.exit(1)

    print(f"📂 {len(todos_jsons)} arquivos encontrados. Processando...")

    for caminho in todos_jsons:
        try:
            with open(caminho, encoding="utf-8") as f:
                envelope = json.load(f)
        except Exception as e:
            print(f"  ⚠️  Erro ao ler {caminho}: {e}")
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

            if fonte not in banco[id_c] or data_ext >= banco[id_c][fonte].get("_data_extracao", ""):
                banco[id_c][fonte] = reg

    return banco


# ---------------------------------------------------------------------------
# Motor de fusão: monta uma linha por idCompra usando todas as fontes
# ---------------------------------------------------------------------------

def _fusionar(id_c: str, fontes: dict[str, dict]) -> dict:
    """
    Monta um registro único priorizando a fonte mais rica para cada campo.
    Prioridade geral: PNCP > LEG_E3 > LEG_E5 > LEG_E1
    Campos extraídos do id_compra são usados como fallback confiável.
    """
    pncp = fontes.get("PNCP",   {})
    e3 = fontes.get("LEG_E3", {})
    e5 = fontes.get("LEG_E5", {})
    e1 = fontes.get("LEG_E1", {})

    master_key = next(
        (k for k in ["PNCP", "LEG_E3", "LEG_E5", "LEG_E1"] if k in fontes), "OUTRO")
    m = fontes[master_key]

    # --- Parse do id_compra (fallback universal) ---
    parsed = _parse_id_compra(id_c)

    # --- Metadados ---
    modulo = "LEI14133" if master_key == "PNCP" else "LEGADO"
    arquivo = m.get("_arquivo_origem", "")
    data_ex = m.get("_data_extracao", "")

    # --- UASG: dicionário do config é a fonte primária ---
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

    # --- Número da compra: extraído do id_compra como fonte primária ---
    numero_compra = _primeiro(
        parsed.get("numero_compra"),          # '000021/2016'  ← do id_compra
        pncp.get("numeroCompra"),
        e3.get("nu_pregao_original"), e3.get("nu_aviso_licitacao"),
        e5.get("nu_aviso_licitacao"), e1.get("numero_aviso"),
    )

    # --- Modalidade: id_compra garante código e nome mesmo sem dados da API ---
    cod_mod = _primeiro(
        pncp.get("codigoModalidade"),
        m.get("co_modalidade_licitacao"), e1.get("modalidade"),
        parsed.get("modalidade_codigo"),  # ← do id_compra
    )
    try:
        cod_mod_int = int(cod_mod)
    except (ValueError, TypeError):
        cod_mod_int = None

    # O dicionário tem prioridade absoluta: ignora o texto livre da API
    # (que retorna variações como "Pregão - Eletrônico", "PREGÃO", etc.)
    nome_mod = MODALIDADES.get(cod_mod_int) or _primeiro(
        e1.get("nome_modalidade"),
        pncp.get("modalidadeNome"),
    )

    # --- Objeto ---
    objeto = _limpar(_primeiro(
        pncp.get("objetoCompra"),
        e1.get("objeto"),
        e5.get("ds_objeto_licitacao"),
        e3.get("ds_objeto"), e3.get("ds_objeto_licitacao"),
        e3.get("objeto"), e3.get("no_objeto"),
        m.get("tx_objeto"), m.get("ds_justificativa"),
    ))

    # --- Responsáveis (exclusivos do Legado) ---
    responsavel_declaracao = _limpar(_primeiro(
        e5.get("no_responsavel_decl_disp"),
        e1.get("no_responsavel_decl_disp"),
        e1.get("nome_responsavel"),
        m.get("no_responsavel_decl_disp"),
    ))
    cargo_declaracao = _limpar(_primeiro(
        e5.get("no_cargo_resp_decl_disp"),
        e1.get("no_cargo_resp_decl_disp"),
        e1.get("funcao_responsavel"),
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
        "data_extracao":              data_ex,
        "id_compra":                  id_c,
        "numero_processo":            _formatar_processo(num_processo),
        "numero_compra":              numero_compra,
        "numero_controle_pncp":       pncp.get("numeroControlePNCP", ""),
        "uasg_codigo":                uasg_codigo,
        "uasg_nome":                  uasg_nome,
        "uasg_sigla":                 _SIGLA_POR_UASG.get(uasg_codigo, ""),
        "modalidade_codigo":          cod_mod,
        "modalidade_nome":            nome_mod,
        "amparo_legal":               _limpar(_primeiro(
            pncp.get("amparoLegalNome"),
            e5.get("ds_fundamento_legal"),
            e3.get("ds_fundamento_legal"),
        )),
        "modo_disputa":               _primeiro(
            pncp.get("modoDisputaNomePncp"),
            e1.get("tipo_pregao"), e3.get("tipo_pregao"),
        ).capitalize(),
        "situacao":                   _primeiro(
            pncp.get("situacaoCompraNomePncp"),
            e3.get("ds_situacao_pregao"),
            e1.get("situacao_aviso"),
        ).capitalize().replace("Suspensa", "Suspenso"),
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
        print("⚠️  Nenhum registro válido encontrado.")
        sys.exit(1)

    os.makedirs(os.path.dirname(caminho_saida) or ".", exist_ok=True)

    with open(caminho_saida, "w", newline="", encoding=EXPORT_CONFIG["encoding"]) as f:
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

    print(f"\n✅ CSV gerado: {caminho_saida}")
    print(f"   IDs únicos          : {len(registros)}")
    print(f"   Módulo LEGADO       : {total_legado}")
    print(f"   Módulo LEI14133     : {total_14133}")
    print(f"   Com responsável     : {com_resp}")
    print(f"   Sem objeto (API)    : {sem_objeto}")
    print(f"   Colunas             : {len(COLUNAS)}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args():
    parser = argparse.ArgumentParser(
        description="Transforma JSONs de cache em CSV consolidado para Power BI."
    )
    parser.add_argument(
        "--pasta", nargs="+",
        default=[CONFIG_APIS["LEGADO"]["pasta_cache"],
                 CONFIG_APIS["LEI14133"]["pasta_cache"]],
        help="Pastas com os JSONs (padrão: config.py)",
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
