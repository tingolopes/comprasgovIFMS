"""
diagnosticar_id.py
------------------
Diagnóstico completo para um idCompra específico.
Mostra em quais arquivos de compras ele aparece, quais endpoints
de itens seriam gerados pela lógica atual, e quais arquivos de
itens já existem em cache.

Uso:
    python diagnosticar_id.py 15813206000472022
"""

import argparse
import glob
import json
import os

PASTA_COMPRAS = r"temp\compras"
PASTA_ITENS = r"temp\compras_itens"


def _carregar_json(caminho: str) -> dict:
    try:
        with open(caminho, encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception as e:
        return {"_erro": str(e)}


def diagnosticar(id_alvo: str) -> None:
    print()
    print("=" * 65)
    print(f"  DIAGNÓSTICO — idCompra: {id_alvo}")
    print("=" * 65)

    # -----------------------------------------------------------------------
    # PASSO 1: onde o ID aparece nos arquivos de COMPRAS
    # -----------------------------------------------------------------------
    print(f"\n📂 PASSO 1 — Arquivos em '{PASTA_COMPRAS}' que contêm o ID\n")

    urls_encontradas: set[str] = set()
    arquivos_encontrados = []

    jsons_compras = sorted(glob.glob(f"{PASTA_COMPRAS}/*.json"))
    if not jsons_compras:
        print(f"  ⚠️  Pasta '{PASTA_COMPRAS}' vazia ou não encontrada.")
    else:
        for caminho in jsons_compras:
            dados = _carregar_json(caminho)
            if dados.get("metadata", {}).get("status") != "SUCESSO":
                continue
            url = dados.get("metadata", {}).get("url_consultada", "")
            respostas = dados.get("respostas", {})
            if not isinstance(respostas, dict):
                continue
            for compra in respostas.get("resultado", []):
                id_c = compra.get("idCompra") or compra.get("id_compra")
                if str(id_c) == id_alvo:
                    urls_encontradas.add(url)
                    arquivos_encontrados.append(os.path.basename(caminho))
                    break   # uma vez por arquivo é suficiente

    if not arquivos_encontrados:
        print(
            f"  ❌ ID '{id_alvo}' NÃO encontrado em nenhum arquivo de compras!")
        print(f"     → O extrator de itens nunca vai gerar tarefas para ele.")
        print(f"     → Verifique se o arquivo de compras correspondente existe")
        print(f"       e se seu status é SUCESSO.\n")
        return

    print(f"  ✅ Encontrado em {len(arquivos_encontrados)} arquivo(s):\n")
    for nome in arquivos_encontrados:
        print(f"    📄 {nome}")
    print()
    print("  URLs consultadas associadas:")
    for url in sorted(urls_encontradas):
        # Extrai só o path para facilitar leitura
        path = url.split("?")[0].split("/")[-1]
        print(f"    🔗 {path}")
        print(f"       {url[:100]}{'...' if len(url) > 100 else ''}")

    # -----------------------------------------------------------------------
    # PASSO 2: quais sufixos a lógica atual geraria
    # -----------------------------------------------------------------------
    print(f"\n🧠 PASSO 2 — Sufixos que a lógica de roteamento geraria\n")

    sufixos_esperados: set[str] = set()

    if any("1_consultarContratacoes_PNCP" in u for u in urls_encontradas):
        sufixos_esperados.add("pncp")
        print("  ✅ pncp   ← apareceu em 1_consultarContratacoes_PNCP")

    if any("5_consultarComprasSemLicitacao" in u for u in urls_encontradas):
        sufixos_esperados.add("E6")
        print("  ✅ E6     ← apareceu em 5_consultarComprasSemLicitacao")

    if any("3_consultarPregoes" in u for u in urls_encontradas):
        sufixos_esperados.add("E2")
        sufixos_esperados.add("E4")
        print("  ✅ E2     ← apareceu em 3_consultarPregoes")
        print("  ✅ E4     ← apareceu em 3_consultarPregoes")

    if any(
        "modulo-legado" in u
        and "3_consultarPregoes" not in u
        and "5_consultarComprasSemLicitacao" not in u
        for u in urls_encontradas
    ):
        sufixos_esperados.add("E2")
        print("  ✅ E2     ← apareceu em outro endpoint legado (outrasmodalidades)")

    if not sufixos_esperados:
        print("  ⚠️  Nenhum sufixo gerado! URLs não reconhecidas pelo roteador.")

    # -----------------------------------------------------------------------
    # PASSO 3: quais arquivos de itens já existem em cache
    # -----------------------------------------------------------------------
    print(f"\n💾 PASSO 3 — Arquivos de itens em '{PASTA_ITENS}'\n")

    todos_sufixos = {"E2", "E4", "E6", "pncp"}
    for sufixo in sorted(todos_sufixos):
        # Verifica página 1 (e mais se existirem)
        pagina = 1
        while True:
            nome = f"itens_{id_alvo}_{sufixo}_p{pagina}.json"
            caminho = os.path.join(PASTA_ITENS, nome)
            if not os.path.exists(caminho):
                if pagina == 1:
                    esperado = "✅ esperado" if sufixo in sufixos_esperados else "  (não esperado)"
                    print(f"  ❌ {nome}  ← NÃO EXISTE  {esperado}")
                break
            dados = _carregar_json(caminho)
            status = dados.get("metadata", {}).get("status", "?")
            n_itens = len(dados.get("respostas", {}).get("resultado") or [])
            icone = "✅" if status == "SUCESSO" else "❌"
            print(f"  {icone} {nome}  status={status}  itens={n_itens}")
            pagina += 1

    # -----------------------------------------------------------------------
    # PASSO 4: diagnóstico final
    # -----------------------------------------------------------------------
    print(f"\n🔎 PASSO 4 — Diagnóstico\n")

    faltando = [s for s in sufixos_esperados
                if not os.path.exists(os.path.join(PASTA_ITENS, f"itens_{id_alvo}_{s}_p1.json"))]

    nao_esperados_existentes = [
        s for s in todos_sufixos - sufixos_esperados
        if os.path.exists(os.path.join(PASTA_ITENS, f"itens_{id_alvo}_{s}_p1.json"))
    ]

    if not faltando:
        print("  ✅ Todos os arquivos esperados existem.")
    else:
        print(f"  ⚠️  Faltam arquivos para: {faltando}")
        print("     Causas possíveis:")
        print("     1. O extrator de itens ainda não rodou após a correção do roteamento")
        print("     2. O ID aparecia em arquivo de compras com status FALHA (foi ignorado)")
        print("     3. A fila foi montada antes da correção e o cache de fila não existe")

    if nao_esperados_existentes:
        print(
            f"\n  ℹ️  Sufixos existentes mas NÃO esperados pelo roteador: {nao_esperados_existentes}")
        print("     → Provavelmente gerados por execução anterior com lógica diferente.")

    print()
    print("=" * 65)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Diagnostica por que um idCompra não recebe determinados endpoints de itens."
    )
    parser.add_argument("id_compra", help="Ex: 15813206000472022")
    parser.add_argument("--pasta-compras", default=PASTA_COMPRAS)
    parser.add_argument("--pasta-itens",   default=PASTA_ITENS)
    args = parser.parse_args()

    PASTA_COMPRAS = args.pasta_compras
    PASTA_ITENS = args.pasta_itens

    diagnosticar(args.id_compra)
