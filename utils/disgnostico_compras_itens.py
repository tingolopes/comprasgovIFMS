"""
utils/diagnostico_compras_itens.py
------------------------------------
Analisa todos os arquivos de temp/compras_itens/ para detectar:

  1. IDs com loop infinito de paginação
     (API retorna paginasRestantes > 0 mas os itens se repetem indefinidamente)

  2. IDs com páginas duplicadas
     (mesmo conjunto de idCompraItem em páginas diferentes)

  3. IDs com buracos de paginação
     (ex: p1, p2, p4 — falta p3)

  4. Resumo geral por ID e sufixo

Uso:
    python utils/diagnostico_compras_itens.py
    python utils/diagnostico_compras_itens.py --pasta caminho/alternativo
    python utils/diagnostico_compras_itens.py --id 15813205901992025
"""

import argparse
import json
import os
import sys
from collections import defaultdict

# Garante que a raiz do projeto está no path
_RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _RAIZ)

PASTA_PADRAO = "temp/compras_itens"


# ---------------------------------------------------------------------------
# Leitura
# ---------------------------------------------------------------------------

def _carregar(caminho: str) -> dict:
    try:
        with open(caminho, encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        print(f"  ⚠️  Erro ao ler {caminho}: {exc}")
        return {}


def _parse_nome(nome: str) -> tuple[str, str, int] | None:
    """
    Extrai (id_compra, sufixo, pagina) do nome do arquivo.
    Formato: itens_{id}_{sufixo}_p{pagina}.json
    """
    try:
        sem_ext = nome.removesuffix(".json")
        partes = sem_ext.split("_")
        # itens | {id} | {sufixo} | p{pagina}
        # sufixo pode ser E2, E4, E6, pncp
        pagina_str = partes[-1]          # p123
        sufixo = partes[-2]          # E2 / pncp / etc
        id_compra = "_".join(partes[1:-2])  # tudo entre itens_ e _{sufixo}
        pagina = int(pagina_str[1:])
        return id_compra, sufixo, pagina
    except Exception:
        return None


def _fingerprint(resultado: list) -> frozenset:
    """Conjunto de idCompraItem — identifica unicamente o conteúdo de uma página."""
    return frozenset(
        str(item.get("idCompraItem") or item.get("numeroItemPncp") or i)
        for i, item in enumerate(resultado)
    )


# ---------------------------------------------------------------------------
# Análise por (id_compra, sufixo)
# ---------------------------------------------------------------------------

def analisar_pasta(pasta: str, filtro_id: str | None = None) -> None:
    if not os.path.exists(pasta):
        print(f"❌ Pasta não encontrada: {pasta}")
        sys.exit(1)

    arquivos = sorted(f for f in os.listdir(pasta) if f.endswith(".json"))
    print(f"📂 {len(arquivos)} arquivo(s) em {pasta}\n")

    # Agrupa por (id_compra, sufixo)
    grupos: dict[tuple, dict[int, dict]] = defaultdict(dict)

    for nome in arquivos:
        parsed = _parse_nome(nome)
        if not parsed:
            print(f"  ⚠️  Nome não reconhecido: {nome}")
            continue
        id_c, sufixo, pagina = parsed
        if filtro_id and id_c != filtro_id:
            continue
        dados = _carregar(os.path.join(pasta, nome))
        grupos[(id_c, sufixo)][pagina] = dados

    if not grupos:
        msg = f"para o ID '{filtro_id}'" if filtro_id else "na pasta"
        print(f"  ℹ️  Nenhum arquivo encontrado {msg}.")
        return

    # --- Resultados ---
    loops: list[tuple] = []
    duplicatas: list[tuple] = []
    buracos: list[tuple] = []
    resumo_ids: dict[str, dict] = defaultdict(
        lambda: {"sufixos": [], "paginas": 0, "itens": 0})

    sep = "-" * 72

    for (id_c, sufixo), paginas in sorted(grupos.items()):
        nums_paginas = sorted(paginas.keys())
        max_pag = max(nums_paginas)
        total_arqs = len(nums_paginas)

        # --- Fingerprints por página ---
        fps: dict[int, frozenset] = {}
        itens_por_pag: dict[int, int] = {}
        pag_rest_por_pag: dict[int, int] = {}

        for p, dados in paginas.items():
            res = dados.get("respostas", {})
            resultado = res.get("resultado", []) or [
            ] if isinstance(res, dict) else []
            fps[p] = _fingerprint(resultado)
            itens_por_pag[p] = len(resultado)
            pag_rest_por_pag[p] = int(
                res.get("paginasRestantes") or 0) if isinstance(res, dict) else 0

        total_itens = sum(itens_por_pag.values())

        # --- Detecta buracos ---
        esperadas = set(range(1, max_pag + 1))
        faltando = sorted(esperadas - set(nums_paginas))
        if faltando:
            buracos.append((id_c, sufixo, faltando))

        # --- Detecta duplicatas de conteúdo ---
        fp_para_pags: dict[frozenset, list[int]] = defaultdict(list)
        for p, fp in fps.items():
            if fp:  # ignora páginas vazias
                fp_para_pags[fp].append(p)

        pags_duplicadas = {
            pags[0]: pags[1:]
            for pags in fp_para_pags.values()
            if len(pags) > 1
        }

        # --- Detecta loop infinito ---
        # Loop = última página tem paginasRestantes > 0 E conteúdo igual à penúltima
        ultima_pag = max_pag
        penultima = max_pag - 1
        is_loop = False
        if (ultima_pag in fps and penultima in fps
                and fps[ultima_pag] == fps[penultima]
                and pag_rest_por_pag.get(ultima_pag, 0) > 0):
            is_loop = True
            loops.append((id_c, sufixo, ultima_pag,
                         pag_rest_por_pag[ultima_pag]))

        if pags_duplicadas:
            duplicatas.append((id_c, sufixo, pags_duplicadas))

        # --- Resumo por ID ---
        resumo_ids[id_c]["sufixos"].append(sufixo)
        resumo_ids[id_c]["paginas"] += total_arqs
        resumo_ids[id_c]["itens"] += total_itens

        # --- Imprime detalhes se há problema ou se filtro ativo ---
        tem_problema = is_loop or bool(pags_duplicadas) or bool(faltando)
        if filtro_id or tem_problema:
            print(sep)
            print(f"🔑 {id_c} | sufixo: {sufixo}")
            print(
                f"   Páginas armazenadas : {nums_paginas[0]}–{max_pag} ({total_arqs} arquivos)")
            print(f"   Itens totais (bruto): {total_itens}")

            if is_loop:
                print(
                    f"   🔴 LOOP INFINITO     : última página={ultima_pag} | paginasRestantes={pag_rest_por_pag[ultima_pag]}")
            if pags_duplicadas:
                for orig, cópias in sorted(pags_duplicadas.items()):
                    print(
                        f"   🟡 CONTEÚDO DUPLICADO: p{orig} = p{', p'.join(str(c) for c in cópias)}")
            if faltando:
                print(f"   🟠 BURACOS           : páginas {faltando} ausentes")
            if not tem_problema:
                print(f"   ✅ Sem anomalias detectadas")

            # Mostra as últimas 3 páginas se há loop
            if is_loop and len(nums_paginas) >= 2:
                print(f"\n   Últimas páginas ({min(3, len(nums_paginas))}):")
                for p in nums_paginas[-3:]:
                    prest = pag_rest_por_pag.get(p, "?")
                    nitens = itens_por_pag.get(p, 0)
                    fp_hex = hash(fps[p]) % 0xFFFF
                    print(
                        f"     p{p}: {nitens} item(ns) | paginasRestantes={prest} | fingerprint=#{fp_hex:04X}")

    # ---------------------------------------------------------------------------
    # Sumário global
    # ---------------------------------------------------------------------------
    print(f"\n{'=' * 72}")
    print("📊 SUMÁRIO GLOBAL")
    print(f"{'=' * 72}")
    print(f"  IDs únicos analisados : {len(resumo_ids)}")
    print(f"  IDs com loop infinito : {len(loops)}")
    print(
        f"  IDs com duplicatas    : {len(set(id_c for id_c, *_ in duplicatas))}")
    print(
        f"  IDs com buracos       : {len(set(id_c for id_c, *_ in buracos))}")

    if loops:
        print(f"\n🔴 IDs COM LOOP INFINITO ({len(loops)}):")
        for id_c, sufixo, ultima, prest in loops:
            paginas_extras = grupos[(id_c, sufixo)]
            n_arqs = len(paginas_extras)
            print(
                f"   {id_c} [{sufixo}] — {n_arqs} arquivos | última p{ultima} | paginasRestantes={prest}")

    if duplicatas:
        print(
            f"\n🟡 IDs COM CONTEÚDO DUPLICADO ({len(set(id_c for id_c, *_ in duplicatas))}):")
        for id_c, sufixo, pags in duplicatas:
            for orig, copias in sorted(pags.items()):
                print(
                    f"   {id_c} [{sufixo}] — p{orig} duplicada em p{', p'.join(str(c) for c in copias)}")

    if buracos:
        print(
            f"\n🟠 IDs COM BURACOS ({len(set(id_c for id_c, *_ in buracos))}):")
        for id_c, sufixo, faltando in buracos:
            print(f"   {id_c} [{sufixo}] — faltam: {faltando}")

    # --- Top 10 por número de arquivos ---
    print(f"\n📈 TOP 10 IDs POR NÚMERO DE ARQUIVOS:")
    top = sorted(resumo_ids.items(),
                 key=lambda x: x[1]["paginas"], reverse=True)[:10]
    for id_c, info in top:
        print(
            f"   {id_c} — {info['paginas']} arquivo(s) | sufixos: {info['sufixos']} | itens brutos: {info['itens']}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Diagnostica a pasta temp/compras_itens em busca de loops, duplicatas e buracos."
    )
    parser.add_argument("--pasta", default=PASTA_PADRAO,
                        help=f"Pasta a analisar (padrão: {PASTA_PADRAO})")
    parser.add_argument("--id", dest="filtro_id", default=None,
                        help="Analisa somente um idCompra específico")
    args = parser.parse_args()

    analisar_pasta(args.pasta, filtro_id=args.filtro_id)
