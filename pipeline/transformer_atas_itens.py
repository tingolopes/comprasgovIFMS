"""
pipeline/transformer_atas_itens.py
------------------------------------
Varre temp/atas_itens/ e gera atas_itens.csv consolidado para Power BI.

Deduplicação por (numeroControlePncpAta + numeroItem) — a mesma ata pode
aparecer em múltiplos arquivos de janelas anuais diferentes.
Em caso de duplicata, mantém o registro com data_extracao mais recente.

Uso como módulo:
    from pipeline.transformer_atas_itens import transformar
    transformar()

Uso via CLI:
    python -m pipeline.transformer_atas_itens
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

_PASTA_ITENS = CONFIG_ATAS["pasta_cache_itens"]

# ---------------------------------------------------------------------------
# Schema do CSV final
# ---------------------------------------------------------------------------
COLUNAS = [
    # Rastreabilidade
    "arquivo_origem",
    "data_extracao",

    # Identificação
    "numero_ata",
    "numero_controle_pncp_ata",
    "numero_controle_pncp_compra",
    "id_compra",
    "numero_item",

    # Compra origem
    "numero_compra",
    "ano_compra",
    "modalidade_codigo",
    "modalidade_nome",

    # Unidade gerenciadora
    "uasg_codigo",
    "uasg_nome",

    # Item
    "codigo_item",
    "codigo_pdm",
    "nome_pdm",
    "tipo_item",
    "descricao_item",

    # Fornecedor
    "fornecedor_cnpj",
    "fornecedor_nome",
    "classificacao_fornecedor",
    "situacao_sicaf",

    # Quantidades e valores
    "quantidade_homologada_item",
    "quantidade_homologada_vencedor",
    "quantidade_empenhada",
    "maximo_adesao",
    "valor_unitario",
    "valor_total",
    "percentual_maior_desconto",

    # Vigência
    "data_assinatura",
    "data_vigencia_inicial",
    "data_vigencia_final",

    # Controle
    "item_excluido",
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


def _parse_id_compra(id_compra: str) -> tuple[str, str]:
    """
    Extrai (numero_compra, ano_compra) do idCompra da ata.
    Formato: UASG(6) + meio(2) + numero_compra(5) + ano(4)
    Exemplo: "15813205902412025" → ("90241", "2025")
    """
    try:
        if len(id_compra) >= 13:
            numero = str(int(id_compra[-9:-4]))  # remove zeros à esquerda
            ano = id_compra[-4:]
            return numero, ano
    except Exception:
        pass
    return "", ""


def _carregar_mapa_atas(caminho_atas_csv: str) -> tuple[dict[str, str], set[str]]:
    """
    Lê atas.csv e retorna:
      - { numeroControlePncpCompra: id_compra }  → enriquece itens sem id_compra
      - set de numeroControlePncpAta conhecidos   → detecta atas ausentes
    """
    mapa: dict[str, str] = {}
    ctrl_atas_conhecidas: set[str] = set()

    if not os.path.exists(caminho_atas_csv):
        print(
            f"  ℹ️  atas.csv não encontrado em {caminho_atas_csv} — id_compra não será enriquecido.")
        return mapa, ctrl_atas_conhecidas
    try:
        import csv as _csv
        with open(caminho_atas_csv, encoding=EXPORT_CONFIG["encoding"]) as f:
            reader = _csv.DictReader(f, delimiter=EXPORT_CONFIG["separador"])
            for row in reader:
                ctrl_compra = row.get(
                    "numero_controle_pncp_compra", "").strip()
                id_c = row.get("id_compra", "").strip()
                ctrl_ata = row.get("numero_controle_pncp_ata", "").strip()
                if ctrl_compra and id_c:
                    mapa[ctrl_compra] = id_c
                if ctrl_ata:
                    ctrl_atas_conhecidas.add(ctrl_ata)
        print(
            f"  ✅ Mapa de atas carregado: {len(mapa)} compras | {len(ctrl_atas_conhecidas)} atas conhecidas")
    except Exception as exc:
        print(f"  ⚠️  Erro ao carregar atas.csv: {exc}")
    return mapa, ctrl_atas_conhecidas


# ---------------------------------------------------------------------------
# Indexação — deduplicação por (numeroControlePncpAta + numeroItem)
# ---------------------------------------------------------------------------

def _indexar() -> dict[str, dict]:
    """
    Retorna { "ctrl_pncp_ata|numero_item": registro } — deduplicado.
    Em caso de duplicata mantém o registro com data_extracao mais recente.
    """
    banco: dict[str, dict] = {}

    jsons = sorted(glob.glob(f"{_PASTA_ITENS}/*.json"))
    if not jsons:
        print(f"⚠️  Nenhum JSON em {_PASTA_ITENS}")
        return banco

    print(f"📂 {len(jsons)} arquivo(s) de itens de atas encontrado(s). Processando...")

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

            ctrl_ata = str(reg.get("numeroControlePncpAta") or "")
            num_item = str(reg.get("numeroItem") or "")

            if not ctrl_ata or not num_item:
                continue

            chave = f"{ctrl_ata}|{num_item}"
            reg["_arquivo_origem"] = arquivo
            reg["_data_extracao"] = data_ext

            if chave not in banco:
                banco[chave] = reg
            else:
                # Mantém o mais recente
                if data_ext > banco[chave].get("_data_extracao", ""):
                    banco[chave] = reg

    return banco


# ---------------------------------------------------------------------------
# Mapeamento de campos
# ---------------------------------------------------------------------------

def _mapear(reg: dict, mapa_atas: dict[str, str]) -> dict:
    ctrl_compra = reg.get("numeroControlePncpCompra", "") or ""

    # id_compra: usa o da API; se vazio, busca no mapa de atas.csv
    id_compra = str(reg.get("idCompra") or "").strip()
    if not id_compra and ctrl_compra:
        id_compra = mapa_atas.get(ctrl_compra, "")

    # numero_compra e ano_compra: usa os da API; se vazios, extrai do id_compra
    numero_compra = str(reg.get("numeroCompra") or "").strip()
    ano_compra = str(reg.get("anoCompra") or "").strip()
    if (not numero_compra or not ano_compra) and id_compra:
        num_parsed, ano_parsed = _parse_id_compra(id_compra)
        if not numero_compra:
            numero_compra = num_parsed
        if not ano_compra:
            ano_compra = ano_parsed

    return {
        "arquivo_origem":              reg.get("_arquivo_origem", ""),
        "data_extracao":               reg.get("_data_extracao", ""),
        "numero_ata":                  reg.get("numeroAtaRegistroPreco", ""),
        "numero_controle_pncp_ata":    reg.get("numeroControlePncpAta", ""),
        "numero_controle_pncp_compra": ctrl_compra,
        "id_compra":                   id_compra,
        "numero_item":                 reg.get("numeroItem", ""),
        "numero_compra":               numero_compra,
        "ano_compra":                  ano_compra,
        "modalidade_codigo":           reg.get("codigoModalidadeCompra", ""),
        "modalidade_nome":             reg.get("nomeModalidadeCompra", ""),
        "uasg_codigo":                 reg.get("codigoUnidadeGerenciadora", ""),
        "uasg_nome":                   _limpar(reg.get("nomeUnidadeGerenciadora")),
        "codigo_item":                 reg.get("codigoItem", ""),
        "codigo_pdm":                  reg.get("codigoPdm", "") if reg.get("codigoPdm") is not None else "",
        "nome_pdm":                    _limpar(reg.get("nomePdm")),
        "tipo_item":                   reg.get("tipoItem", ""),
        "descricao_item":              _limpar(reg.get("descricaoItem")),
        "fornecedor_cnpj":             reg.get("niFornecedor", ""),
        "fornecedor_nome":             _limpar(reg.get("nomeRazaoSocialFornecedor")),
        "classificacao_fornecedor":    reg.get("classificacaoFornecedor", ""),
        "situacao_sicaf":              reg.get("situacaoSicaf", ""),
        "quantidade_homologada_item":  _valor(reg.get("quantidadeHomologadaItem")),
        "quantidade_homologada_vencedor": _valor(reg.get("quantidadeHomologadaVencedor")),
        "quantidade_empenhada":        _valor(reg.get("quantidadeEmpenhada")),
        "maximo_adesao":               _valor(reg.get("maximoAdesao")),
        "valor_unitario":              _valor(reg.get("valorUnitario")),
        "valor_total":                 _valor(reg.get("valorTotal")),
        "percentual_maior_desconto":   _valor(reg.get("percentualMaiorDesconto")),
        "data_assinatura":             _data(reg.get("dataAssinatura")),
        "data_vigencia_inicial":       _data(reg.get("dataVigenciaInicial")),
        "data_vigencia_final":         _data(reg.get("dataVigenciaFinal")),
        "item_excluido":               _bool_str(reg.get("itemExcluido")),
        "data_inclusao":               _data(reg.get("dataHoraInclusao")),
        "data_atualizacao":            _data(reg.get("dataHoraAtualizacao")),
    }


# ---------------------------------------------------------------------------
# Atas manuais — reconstrói esqueleto para atas ausentes do atas.csv
# ---------------------------------------------------------------------------

def _criar_atas_manuais(
    banco: dict[str, dict],
    ctrl_atas_conhecidas: set[str],
    mapa_compras: dict[str, str],
    pasta_atas: str,
) -> int:
    """
    Detecta atas referenciadas nos itens mas ausentes do atas.csv.
    Para cada uma, reconstrói o esqueleto a partir dos campos do item
    e salva/atualiza temp/atas/atas_manuais.json.

    Retorna o número de atas novas adicionadas.
    """
    # Agrupa os itens por ctrl_ata — pega o primeiro item de cada ata como fonte
    atas_nos_itens: dict[str, dict] = {}
    for reg in banco.values():
        ctrl_ata = str(reg.get("numeroControlePncpAta") or "")
        if ctrl_ata and ctrl_ata not in atas_nos_itens:
            atas_nos_itens[ctrl_ata] = reg

    # Identifica as ausentes
    ausentes = {
        ctrl: reg
        for ctrl, reg in atas_nos_itens.items()
        if ctrl not in ctrl_atas_conhecidas
    }

    if not ausentes:
        print("  ✅ Nenhuma ata ausente detectada.")
        return 0

    print(
        f"  ⚠️  {len(ausentes)} ata(s) ausente(s) do atas.csv — gerando esqueletos...")

    # Carrega atas_manuais.json existente (se houver) para não perder entradas anteriores
    caminho_manual = os.path.join(pasta_atas, "atas_manuais.json")
    existente: list[dict] = []
    ctrl_ja_no_manual: set[str] = set()

    if os.path.exists(caminho_manual):
        try:
            with open(caminho_manual, encoding="utf-8") as f:
                env = json.load(f)
            existente = env.get("respostas", {}).get("resultado", []) or []
            ctrl_ja_no_manual = {
                str(r.get("numeroControlePncpAta") or "")
                for r in existente
            }
        except Exception as exc:
            print(f"  ⚠️  Erro ao ler atas_manuais.json: {exc}")

    novas: list[dict] = []
    for ctrl_ata, reg in sorted(ausentes.items()):
        if ctrl_ata in ctrl_ja_no_manual:
            continue  # já estava no manual de execução anterior

        # Reconstrói o esqueleto com os campos disponíveis no item
        id_compra = str(reg.get("idCompra") or "").strip()
        if not id_compra:
            ctrl_compra = str(
                reg.get("numeroControlePncpCompra") or "").strip()
            id_compra = mapa_compras.get(ctrl_compra, "")

        esqueleto = {
            "numeroAtaRegistroPreco":     reg.get("numeroAtaRegistroPreco", ""),
            "codigoUnidadeGerenciadora":  str(reg.get("codigoUnidadeGerenciadora") or ""),
            "nomeUnidadeGerenciadora":    reg.get("nomeUnidadeGerenciadora", ""),
            "codigoOrgao":                "",
            "nomeOrgao":                  reg.get("nomeUnidadeGerenciadora", ""),
            "linkAtaPNCP":                "",
            "linkCompraPNCP":             "",
            "numeroCompra":               reg.get("numeroCompra", ""),
            "anoCompra":                  str(reg.get("anoCompra") or ""),
            "codigoModalidadeCompra":     str(reg.get("codigoModalidadeCompra") or ""),
            "nomeModalidadeCompra":       reg.get("nomeModalidadeCompra", ""),
            "dataAssinatura":             reg.get("dataAssinatura", ""),
            "dataVigenciaInicial":        reg.get("dataVigenciaInicial", ""),
            "dataVigenciaFinal":          reg.get("dataVigenciaFinal", ""),
            "valorTotal":                 None,
            "statusAta":                  "Ata de Registro de Preços",
            "objeto":                     "Dado não disponível na API do governo",
            "quantidadeItens":            None,
            "dataHoraAtualizacao":        reg.get("dataHoraAtualizacao", ""),
            "dataHoraInclusao":           reg.get("dataHoraInclusao", ""),
            "dataHoraExclusao":           None,
            "ataExcluido":                False,
            "numeroControlePncpAta":      ctrl_ata,
            "numeroControlePncpCompra":   reg.get("numeroControlePncpCompra", ""),
            "idCompra":                   id_compra,
        }
        novas.append(esqueleto)
        print(f"    ➕ {ctrl_ata} (ata {esqueleto['numeroAtaRegistroPreco']})")

    if not novas:
        print("  ✅ Nenhuma ata nova para adicionar ao manual.")
        return 0

    todas = existente + novas
    envelope = {
        "metadata": {
            "url_consultada": "MANUAL — gerado por transformer_atas_itens",
            "data_extracao":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status":         "SUCESSO",
        },
        "respostas": {
            "resultado":       todas,
            "totalRegistros":  len(todas),
            "totalPaginas":    1,
            "paginasRestantes": 0,
        },
    }

    os.makedirs(pasta_atas, exist_ok=True)
    with open(caminho_manual, "w", encoding="utf-8") as f:
        json.dump(envelope, f, ensure_ascii=False, indent=4)

    print(
        f"  💾 atas_manuais.json salvo: {len(todas)} ata(s) total ({len(novas)} nova(s))")
    return len(novas)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def transformar(
    pasta_itens: str = _PASTA_ITENS,
    caminho_saida: Optional[str] = None,
) -> None:
    global _PASTA_ITENS
    _PASTA_ITENS = pasta_itens

    if caminho_saida is None:
        caminho_saida = os.path.join(
            EXPORT_CONFIG["pasta_saida"], "atas_itens.csv")

    # Carrega mapa de atas.csv para enriquecer id_compra e detectar atas ausentes
    caminho_atas = os.path.join(EXPORT_CONFIG["pasta_saida"], "atas.csv")
    mapa_atas, ctrl_atas_conhecidas = _carregar_mapa_atas(caminho_atas)

    banco = _indexar()

    if not banco:
        print("⚠️  Nenhum item de ata válido encontrado.")
        sys.exit(1)

    # Detecta atas ausentes e cria/atualiza atas_manuais.json
    novas_manuais = _criar_atas_manuais(
        banco=banco,
        ctrl_atas_conhecidas=ctrl_atas_conhecidas,
        mapa_compras=mapa_atas,
        pasta_atas=CONFIG_ATAS["pasta_cache"],
    )

    # Se novas atas manuais foram criadas, regenera atas.csv antes de prosseguir
    if novas_manuais > 0:
        print("\n🔄 Regenerando atas.csv com as novas entradas manuais...")
        from pipeline.transformer_atas import transformar as _transformar_atas
        _transformar_atas()
        # Recarrega o mapa atualizado
        mapa_atas, ctrl_atas_conhecidas = _carregar_mapa_atas(caminho_atas)
        print()

    registros = [_mapear(reg, mapa_atas) for reg in banco.values()]

    # Ordena por ata + numero_item para facilitar leitura no Power BI
    registros.sort(key=lambda r: (
        r.get("numero_controle_pncp_ata") or "",
        r.get("numero_item") or "",
    ))

    excluidos = sum(1 for r in registros if r["item_excluido"] == "Sim")
    sem_id_compra = sum(1 for r in registros if not r["id_compra"])
    sem_num_compra = sum(1 for r in registros if not r["numero_compra"])

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

    total_mat = sum(1 for r in registros if r["tipo_item"] == "Material")
    total_serv = sum(1 for r in registros if r["tipo_item"] == "Serviço")

    print(f"\n✅ CSV gerado: {caminho_saida}")
    print(f"   Itens únicos     : {len(registros)}")
    print(f"   Material         : {total_mat}")
    print(f"   Serviço          : {total_serv}")
    print(f"   Excluídos        : {excluidos}")
    print(f"   Sem id_compra    : {sem_id_compra}")
    print(f"   Sem numero_compra: {sem_num_compra}")
    print(f"   Colunas          : {len(COLUNAS)}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Transforma JSONs de itens de atas em atas_itens.csv para Power BI."
    )
    parser.add_argument("--pasta", default=_PASTA_ITENS)
    parser.add_argument("--saida", default=os.path.join(
        EXPORT_CONFIG["pasta_saida"], "atas_itens.csv"))
    args = parser.parse_args()
    transformar(pasta_itens=args.pasta, caminho_saida=args.saida)
