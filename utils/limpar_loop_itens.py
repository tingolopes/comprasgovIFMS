"""
utils/limpar_loops_paginacao.py
---------------------------------
Remove páginas inválidas geradas por loop infinito da API nas pastas de cache.

Detecta dois tipos de anomalia:

  TIPO 1 — Loop por conteúdo duplicado (temp/compras_itens/)
    A API retorna paginasRestantes > 0 mas o resultado é idêntico ao da
    página anterior. A partir da 2ª ocorrência do mesmo conteúdo, apaga.

  TIPO 2 — Loop por resultado vazio (temp/compras/)
    A API retorna resultado: [] mas paginasRestantes > 0. Nunca deveria
    haver páginas seguintes se o resultado já veio vazio. Apaga todas
    as páginas vazias com paginasRestantes > 0.

Modo seguro (padrão): apenas lista o que seria apagado, sem apagar nada.
Modo execução: passa --executar para apagar de verdade.

Uso:
    python utils/limpar_loops_paginacao.py                   # simulação (todas as pastas)
    python utils/limpar_loops_paginacao.py --executar        # apaga de verdade
    python utils/limpar_loops_paginacao.py --pasta temp/compras_itens
    python utils/limpar_loops_paginacao.py --pasta temp/compras
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict

_RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _RAIZ)

# Pastas verificadas por padrão
PASTAS_PADRAO = [
    "temp/compras_itens",
    "temp/compras",
]


# ---------------------------------------------------------------------------
# Helpers comuns
# ---------------------------------------------------------------------------

def _carregar(caminho: str) -> dict:
    try:
        with open(caminho, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _paginas_restantes(dados: dict) -> int:
    res = dados.get("respostas", {})
    if isinstance(res, dict):
        return int(res.get("paginasRestantes") or 0)
    return 0


def _resultado(dados: dict) -> list:
    res = dados.get("respostas", {})
    if isinstance(res, dict):
        return res.get("resultado") or []
    return []


def _apagar_arquivos(caminhos: list[str], dry_run: bool) -> int:
    """Apaga (ou simula) a lista de caminhos. Retorna quantidade removida."""
    if dry_run:
        return len(caminhos)
    removidos = 0
    for c in caminhos:
        try:
            os.remove(c)
            removidos += 1
        except Exception as exc:
            print(f"   ⚠️  Erro ao remover {os.path.basename(c)}: {exc}")
    return removidos


# ---------------------------------------------------------------------------
# TIPO 1 — Loop por conteúdo duplicado (temp/compras_itens/)
# Nomenclatura: itens_{id}_{sufixo}_p{pagina}.json
# ---------------------------------------------------------------------------

def _parse_nome_itens(nome: str) -> tuple[str, str, int] | None:
    """(id_compra, sufixo, pagina) de itens_{id}_{sufixo}_p{pag}.json"""
    try:
        sem_ext = nome.removesuffix(".json")
        partes = sem_ext.split("_")
        pagina = int(partes[-1][1:])
        sufixo = partes[-2]
        id_c = "_".join(partes[1:-2])
        return id_c, sufixo, pagina
    except Exception:
        return None


def _fingerprint_itens(dados: dict) -> frozenset:
    resultado = _resultado(dados)
    return frozenset(
        str(item.get("idCompraItem") or item.get("numeroItemPncp") or i)
        for i, item in enumerate(resultado)
    )


def analisar_tipo1(pasta: str) -> dict[str, list[str]]:
    """
    Detecta loops por conteúdo duplicado.
    Retorna { chave: [caminhos a apagar] }.
    """
    arquivos = sorted(f for f in os.listdir(pasta) if f.endswith(".json"))
    grupos: dict[tuple, dict[int, str]] = defaultdict(dict)

    for nome in arquivos:
        parsed = _parse_nome_itens(nome)
        if not parsed:
            continue
        id_c, sufixo, pagina = parsed
        grupos[(id_c, sufixo)][pagina] = os.path.join(pasta, nome)

    para_apagar: dict[str, list[str]] = {}

    for (id_c, sufixo), paginas in sorted(grupos.items()):
        nums = sorted(paginas.keys())
        if len(nums) < 2:
            continue

        fp_anterior: frozenset | None = None
        inicio_loop: int | None = None

        for p in nums:
            fp = _fingerprint_itens(_carregar(paginas[p]))
            if fp_anterior is not None and fp == fp_anterior and fp:
                inicio_loop = p
                break
            fp_anterior = fp

        if inicio_loop is None:
            continue

        chave = f"{id_c}|{sufixo}"
        para_apagar[chave] = [paginas[p] for p in nums if p >= inicio_loop]

    return para_apagar


# ---------------------------------------------------------------------------
# TIPO 2 — Loop por resultado vazio (temp/compras/)
# Nomenclatura: pncp_{sigla}_{modalidade}_{ano}_p{pagina}.json
#               ou {label}_{sigla}_{ano}_p{pagina}.json
# ---------------------------------------------------------------------------

def _parse_nome_compras(nome: str) -> tuple[str, int] | None:
    """
    Extrai (grupo, pagina) do nome do arquivo de compras.
    Grupo = tudo antes de _p{pagina}.json — identifica a série.
    Ex: pncp_RT_pregao_2023_p646 → grupo=pncp_RT_pregao_2023, pagina=646
    """
    try:
        sem_ext = nome.removesuffix(".json")
        m = re.match(r"^(.+)_p(\d+)$", sem_ext)
        if not m:
            return None
        return m.group(1), int(m.group(2))
    except Exception:
        return None


def analisar_tipo2(pasta: str) -> dict[str, list[str]]:
    """
    Detecta loops por resultado vazio com paginasRestantes > 0.
    Retorna { grupo: [caminhos a apagar] }.
    """
    arquivos = sorted(f for f in os.listdir(pasta) if f.endswith(".json"))
    grupos: dict[str, dict[int, str]] = defaultdict(dict)

    for nome in arquivos:
        parsed = _parse_nome_compras(nome)
        if not parsed:
            continue
        grupo, pagina = parsed
        grupos[grupo][pagina] = os.path.join(pasta, nome)

    para_apagar: dict[str, list[str]] = {}

    for grupo, paginas in sorted(grupos.items()):
        nums = sorted(paginas.keys())
        apagar = []

        for p in nums:
            dados = _carregar(paginas[p])
            vazio = not _resultado(dados)
            pag_r = _paginas_restantes(dados)

            if vazio and pag_r > 0:
                # Resultado vazio mas API diz que tem mais — é loop vazio
                apagar.append(paginas[p])

        if apagar:
            para_apagar[grupo] = apagar

    return para_apagar


# ---------------------------------------------------------------------------
# Relatório de extratores responsáveis
# ---------------------------------------------------------------------------

def _identificar_extrator(grupo: str) -> str:
    """Mapeia o grupo/arquivo para o extrator responsável."""
    # Tipo 1: chave tem formato "id_compra|sufixo"
    if "|" in grupo:
        sufixo = grupo.split("|")[-1]
        mapa = {
            "E2":   "extractors_compras_itens.py → E2 [modulo-legado/2.1_consultarItemLicitacao_Id]",
            "E4":   "extractors_compras_itens.py → E4 [modulo-legado/4.1_consultarItensPregoes_Id]",
            "E6":   "extractors_compras_itens.py → E6 [modulo-legado/6.1_consultarItensComprasSemLicitacao_Id]",
            "pncp": "extractors_compras_itens.py → pncp [modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id]",
        }
        return mapa.get(sufixo, f"extractors_compras_itens.py → sufixo {sufixo}")
    if grupo.startswith("pncp_"):
        return "extractors_compras.py → _executar_14133() [módulo-contratacoes PNCP]"
    if grupo.startswith("outrasmodalidades_"):
        return "extractors_compras.py → _executar_legado() [modulo-legado/1_consultarLicitacao]"
    if grupo.startswith("pregao_"):
        return "extractors_compras.py → _executar_legado() [modulo-legado/3_consultarPregoes]"
    if grupo.startswith("dispensa_"):
        return "extractors_compras.py → _executar_legado() [modulo-legado/5_consultarComprasSemLicitacao]"
    if grupo.startswith("itens_") and "_E2" in grupo:
        return "extractors_compras_itens.py → endpoint E2 [modulo-legado/2.1_consultarItemLicitacao_Id]"
    if grupo.startswith("itens_") and "_E4" in grupo:
        return "extractors_compras_itens.py → endpoint E4 [modulo-legado/4.1_consultarItensPregoes_Id]"
    if grupo.startswith("itens_") and "_E6" in grupo:
        return "extractors_compras_itens.py → endpoint E6 [modulo-legado/6.1_consultarItensComprasSemLicitacao_Id]"
    if grupo.startswith("itens_") and "_pncp" in grupo:
        return "extractors_compras_itens.py → endpoint pncp [modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id]"
    return "extrator desconhecido"


# ---------------------------------------------------------------------------
# Execução principal
# ---------------------------------------------------------------------------

def executar_pasta(pasta: str, dry_run: bool) -> tuple[int, int]:
    """
    Analisa uma pasta e remove anomalias.
    Retorna (grupos_afetados, arquivos_removidos).
    """
    if not os.path.exists(pasta):
        print(f"  ⚠️  Pasta não encontrada: {pasta} — pulando.")
        return 0, 0

    # Detecta tipo pela pasta/padrão de nomes
    arquivos = [f for f in os.listdir(pasta) if f.endswith(".json")]
    tem_itens = any(f.startswith("itens_") for f in arquivos)
    tem_compras = any(not f.startswith("itens_") for f in arquivos)

    resultados: dict[str, list[str]] = {}

    if tem_itens:
        resultados.update(analisar_tipo1(pasta))
    if tem_compras:
        resultados.update(analisar_tipo2(pasta))

    if not resultados:
        print(f"  ✅ Sem anomalias em {pasta}")
        return 0, 0

    total_arqs = sum(len(v) for v in resultados.values())
    print(f"\n  {'=' * 64}")
    print(f"  Pasta   : {pasta}")
    print(f"  Grupos  : {len(resultados)}")
    print(f"  Arquivos: {total_arqs}")
    print(f"  {'=' * 64}")

    # Agrupa por extrator para o relatório
    por_extrator: dict[str, list[str]] = defaultdict(list)

    for chave, caminhos in sorted(resultados.items()):
        extrator = _identificar_extrator(chave)
        por_extrator[extrator].append(chave)

        nomes = [os.path.basename(c) for c in caminhos]
        p_ini = re.search(r"_p(\d+)\.json$", nomes[0])
        p_fim = re.search(r"_p(\d+)\.json$", nomes[-1])
        p_ini_s = f"p{p_ini.group(1)}" if p_ini else "?"
        p_fim_s = f"p{p_fim.group(1)}" if p_fim else "?"

        print(f"\n  🔑 {chave}")
        print(
            f"     Arquivos: {p_ini_s}..{p_fim_s} ({len(caminhos)} arquivo(s))")
        print(f"     Extrator: {extrator}")

        removidos = _apagar_arquivos(caminhos, dry_run)
        if dry_run:
            print(
                f"     🔵 [simulação] {removidos} arquivo(s) seriam removidos")
        else:
            print(
                f"     {'✅' if removidos == len(caminhos) else '⚠️ '} {removidos}/{len(caminhos)} removido(s)")

    # Relatório de extratores
    print(f"\n  📋 EXTRATORES RESPONSÁVEIS:")
    for extrator, chaves in sorted(por_extrator.items()):
        print(f"     • {extrator}")
        print(f"       Séries afetadas: {len(chaves)}")

    return len(resultados), total_arqs


def executar(pastas: list[str], dry_run: bool) -> None:
    modo = "SIMULAÇÃO (sem apagar nada)" if dry_run else "EXECUÇÃO REAL"
    print(f"\n{'=' * 68}")
    print(f"🧹 LIMPEZA DE LOOPS DE PAGINAÇÃO [{modo}]")
    print(f"{'=' * 68}")

    total_grupos = total_arqs = 0
    for pasta in pastas:
        g, a = executar_pasta(pasta, dry_run)
        total_grupos += g
        total_arqs += a

    print(f"\n{'=' * 68}")
    print(f"📊 RESUMO FINAL")
    print(f"   Pastas analisadas    : {len(pastas)}")
    print(f"   Grupos com anomalia  : {total_grupos}")
    print(
        f"   Arquivos {'removidos' if not dry_run else 'a remover'}    : {total_arqs}")
    if dry_run and total_arqs > 0:
        print(f"\n   ℹ️  Para apagar de verdade, rode com --executar")
    elif not dry_run:
        print(f"\n   ✅ Limpeza concluída!")
    print(f"{'=' * 68}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Remove loops de paginação (conteúdo duplicado ou vazio) das pastas de cache."
    )
    parser.add_argument(
        "--pasta", dest="pastas", action="append", default=None,
        help="Pasta a analisar (pode repetir). Padrão: temp/compras_itens e temp/compras"
    )
    parser.add_argument(
        "--executar", action="store_true",
        help="Apaga os arquivos de verdade (padrão: simulação)"
    )
    args = parser.parse_args()

    pastas = args.pastas if args.pastas else PASTAS_PADRAO
    executar(pastas, dry_run=not args.executar)
