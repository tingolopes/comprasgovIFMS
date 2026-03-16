"""
analisar_cobertura_itens.py
----------------------------
Analisa quais endpoints de itens retornaram dados para cada compra.
Cruza temp/compras/ com temp/compras_itens/ e gera um relatório mostrando
padrões de cobertura por tipo/modalidade de compra.

Uso:
    python analisar_cobertura_itens.py
    python analisar_cobertura_itens.py --pasta-compras temp/compras --pasta-itens temp/compras_itens
    python analisar_cobertura_itens.py --csv cobertura.csv
"""

import argparse
import glob
import json
import os
from collections import defaultdict

PASTA_COMPRAS = "temp/compras"
PASTA_ITENS = "temp/compras_itens"
SUFIXOS = ["E2", "E4", "E6", "pncp"]


# ---------------------------------------------------------------------------
# Leitura de arquivos
# ---------------------------------------------------------------------------

def _ler_json(caminho: str) -> dict:
    try:
        with open(caminho, encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _status_item(id_c: str, sufixo: str, pasta_itens: str) -> str:
    """
    Retorna:
      'com_itens'   — arquivo existe, SUCESSO e resultado não vazio
      'vazio'       — arquivo existe, SUCESSO mas resultado vazio / null
      'falha'       — arquivo existe mas status != SUCESSO
      'ausente'     — arquivo não existe
    """
    caminho = os.path.join(pasta_itens, f"itens_{id_c}_{sufixo}_p1.json")
    if not os.path.exists(caminho):
        return "ausente"
    dados = _ler_json(caminho)
    if dados.get("metadata", {}).get("status") != "SUCESSO":
        return "falha"
    resultado = dados.get("respostas", {})
    if isinstance(resultado, dict):
        resultado = resultado.get("resultado") or []
    return "com_itens" if resultado else "vazio"


# ---------------------------------------------------------------------------
# Coleta de compras
# ---------------------------------------------------------------------------

def _coletar_compras(pasta_compras: str) -> list[dict]:
    """
    Retorna lista de dicts com info de cada compra encontrada nos JSONs de compras.
    """
    compras = {}   # idCompra → dict

    for arq in sorted(os.listdir(pasta_compras)):
        if not arq.endswith(".json"):
            continue
        dados = _ler_json(os.path.join(pasta_compras, arq))
        if dados.get("metadata", {}).get("status") != "SUCESSO":
            continue

        url = dados.get("metadata", {}).get("url_consultada", "")

        # Determina tipo de arquivo pelo nome ou URL
        if "1_consultarContratacoes_PNCP" in url:
            tipo_arquivo = "pncp"
        elif "5_consultarComprasSemLicitacao" in url:
            tipo_arquivo = "dispensa"
        elif "3_consultarPregoes" in url:
            tipo_arquivo = "pregao"
        else:
            tipo_arquivo = "outras"

        respostas = dados.get("respostas", {})
        if not isinstance(respostas, dict):
            continue

        for c in respostas.get("resultado", []):
            id_c = str(c.get("idCompra") or c.get("id_compra") or "")
            if not id_c:
                continue
            if id_c not in compras:
                compras[id_c] = {
                    "idCompra":       id_c,
                    "tipo_arquivo":   tipo_arquivo,
                    "modalidade":     (
                        c.get("noModalidadeLicitacao") or
                        c.get("modalidadeNome") or
                        c.get("noModalidade") or ""
                    ),
                    "uasg":           str(c.get("coUasg") or c.get("uasgCodigo") or ""),
                    "ano":            str(c.get("dtAnoAvisoLicitacao") or c.get("anoCompra") or ""),
                    "arquivos_fonte": set(),
                }
            compras[id_c]["arquivos_fonte"].add(arq)

    return list(compras.values())


# ---------------------------------------------------------------------------
# Análise principal
# ---------------------------------------------------------------------------

def analisar(pasta_compras: str, pasta_itens: str, exportar_csv: str | None) -> None:

    print(f"\n{'='*65}")
    print("  ANÁLISE DE COBERTURA DE ENDPOINTS DE ITENS")
    print(f"{'='*65}\n")

    # 1. Coleta compras
    print(f"📂 Lendo compras de '{pasta_compras}'...")
    compras = _coletar_compras(pasta_compras)
    print(f"   {len(compras)} compras únicas encontradas.\n")

    if not compras:
        print("Nenhuma compra encontrada. Verifique a pasta.")
        return

    # 2. Para cada compra, verifica status em cada endpoint de itens
    print(f"🔍 Verificando cobertura em {pasta_itens}...\n")
    linhas = []
    for c in compras:
        linha = dict(c)
        linha["arquivos_fonte"] = len(c["arquivos_fonte"])
        for sufixo in SUFIXOS:
            linha[f"status_{sufixo}"] = _status_item(
                c["idCompra"], sufixo, pasta_itens)
        linha["endpts_com_itens"] = sum(
            1 for s in SUFIXOS if linha[f"status_{s}"] == "com_itens"
        )
        linha["endpts_vazios"] = sum(
            1 for s in SUFIXOS if linha[f"status_{s}"] == "vazio"
        )
        linhas.append(linha)

    # ---------------------------------------------------------------------------
    # 3. Relatório por padrão de cobertura
    # ---------------------------------------------------------------------------
    print(f"{'='*65}")
    print("  PADRÕES DE COBERTURA (agrupado por tipo de compra)")
    print(f"{'='*65}\n")

    # Agrupa por (tipo_arquivo, combinação de endpoints com itens)
    padroes: dict[str, list] = defaultdict(list)
    for l in linhas:
        combo = tuple(s for s in SUFIXOS if l[f"status_{s}"] == "com_itens")
        chave = f"{l['tipo_arquivo']} → {'+'.join(combo) if combo else '(nenhum)'}"
        padroes[chave].append(l)

    for chave, grupo in sorted(padroes.items(), key=lambda x: -len(x[1])):
        print(f"  {len(grupo):>5}x  {chave}")

    # ---------------------------------------------------------------------------
    # 4. Resumo por endpoint
    # ---------------------------------------------------------------------------
    print(f"\n{'='*65}")
    print("  RESUMO POR ENDPOINT")
    print(f"{'='*65}\n")
    print(
        f"  {'Endpoint':<8} {'com_itens':>10} {'vazio':>8} {'falha':>8} {'ausente':>8}")
    print(f"  {'-'*50}")
    for s in SUFIXOS:
        cnt = defaultdict(int)
        for l in linhas:
            cnt[l[f"status_{s}"]] += 1
        print(
            f"  {s:<8} "
            f"{cnt['com_itens']:>10} "
            f"{cnt['vazio']:>8} "
            f"{cnt['falha']:>8} "
            f"{cnt['ausente']:>8}"
        )

    # ---------------------------------------------------------------------------
    # 5. Compras sem nenhum item em nenhum endpoint
    # ---------------------------------------------------------------------------
    sem_itens = [l for l in linhas if l["endpts_com_itens"] == 0]
    print(f"\n{'='*65}")
    print(f"  COMPRAS SEM ITENS EM NENHUM ENDPOINT: {len(sem_itens)}")
    print(f"{'='*65}\n")
    if sem_itens:
        tipo_cnt: dict[str, int] = defaultdict(int)
        for l in sem_itens:
            tipo_cnt[l["tipo_arquivo"]] += 1
        for tipo, cnt in sorted(tipo_cnt.items(), key=lambda x: -x[1]):
            print(f"  {cnt:>5}x  {tipo}")

    # ---------------------------------------------------------------------------
    # 6. Endpoints nunca usados por tipo de arquivo
    # ---------------------------------------------------------------------------
    print(f"\n{'='*65}")
    print("  ENDPOINTS SEMPRE VAZIOS POR TIPO DE COMPRA")
    print(f"  (candidatos a remover do roteamento)")
    print(f"{'='*65}\n")

    tipos = sorted(set(l["tipo_arquivo"] for l in linhas))
    for tipo in tipos:
        grupo = [l for l in linhas if l["tipo_arquivo"] == tipo]
        for s in SUFIXOS:
            total_com = sum(
                1 for l in grupo if l[f"status_{s}"] == "com_itens")
            total = len(grupo)
            pct = total_com / total * 100 if total else 0
            if total_com == 0:
                print(
                    f"  ⛔  {tipo:<10} × {s}  → 0/{total} compras têm itens  (pode remover)")
            elif pct < 5:
                print(
                    f"  ⚠️   {tipo:<10} × {s}  → {total_com}/{total} ({pct:.1f}%)  (raro)")

    # ---------------------------------------------------------------------------
    # 7. Export CSV opcional
    # ---------------------------------------------------------------------------
    if exportar_csv:
        import csv
        campos = (
            ["idCompra", "tipo_arquivo", "modalidade", "uasg", "ano", "arquivos_fonte"] +
            [f"status_{s}" for s in SUFIXOS] +
            ["endpts_com_itens", "endpts_vazios"]
        )
        with open(exportar_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=campos, extrasaction="ignore")
            w.writeheader()
            w.writerows(linhas)
        print(f"\n💾 CSV exportado: {exportar_csv}  ({len(linhas)} linhas)")

    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Analisa cobertura de endpoints de itens por compra."
    )
    parser.add_argument("--pasta-compras", default=PASTA_COMPRAS)
    parser.add_argument("--pasta-itens",   default=PASTA_ITENS)
    parser.add_argument("--csv",           default=None,
                        help="Exporta resultado detalhado para CSV (ex: cobertura.csv)")
    args = parser.parse_args()

    analisar(args.pasta_compras, args.pasta_itens, args.csv)
