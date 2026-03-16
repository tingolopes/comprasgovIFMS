"""
limpar_itens.py
---------------
Remove arquivos de itens vazios da pasta temp/compras_itens.

Critérios para remoção (TODOS devem ser satisfeitos):
  1. status == "SUCESSO"         — não apaga falhas (podem ser re-tentadas)
  2. resultado == [] ou None     — sem itens de verdade
  3. combinação tipo×endpoint    — sabidamente nunca produz dados

Uso:
    python limpar_itens.py               # mostra o que seria apagado (dry-run)
    python limpar_itens.py --executar    # apaga de verdade
    python limpar_itens.py --executar --pasta-itens temp/compras_itens
"""

import argparse
import glob
import json
import os

PASTA_COMPRAS = "temp/compras"
PASTA_ITENS = "temp/compras_itens"

# Combinações tipo×endpoint que nunca produzem itens (baseado em analisar_cobertura_itens.py)
COMBINACOES_INUTEIS: set[tuple[str, str]] = {
    ("dispensa", "E4"),
    ("outras",   "E6"),
    ("pncp",     "E2"),
    ("pncp",     "E4"),
    ("pncp",     "E6"),
    ("pregao",   "E6"),
    ("pregao",   "pncp"),
}


# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------

def _ler_json(caminho: str) -> dict:
    try:
        with open(caminho, encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _tipo_de_url(url: str) -> str:
    if "1_consultarContratacoes_PNCP" in url:
        return "pncp"
    if "5_consultarComprasSemLicitacao" in url:
        return "dispensa"
    if "3_consultarPregoes" in url:
        return "pregao"
    return "outras"


def _tem_itens(dados: dict) -> bool:
    """Retorna True se o arquivo tem pelo menos 1 item no resultado."""
    respostas = dados.get("respostas", {})
    if isinstance(respostas, dict):
        resultado = respostas.get("resultado") or []
    elif isinstance(respostas, list):
        resultado = respostas
    else:
        resultado = []
    return bool(resultado)


def _sufixo_do_arquivo(nome: str) -> str:
    """Extrai sufixo (E2, E4, E6, pncp) do nome do arquivo de itens."""
    partes = os.path.basename(nome).replace(".json", "").split("_")
    return partes[-2] if len(partes) >= 3 else ""


# ---------------------------------------------------------------------------
# Mapeia idCompra → tipo(s) a partir dos arquivos de compras
# ---------------------------------------------------------------------------

def _mapear_tipos(pasta_compras: str) -> dict[str, set[str]]:
    tipos: dict[str, set[str]] = {}
    if not os.path.exists(pasta_compras):
        return tipos
    for arq in sorted(os.listdir(pasta_compras)):
        if not arq.endswith(".json"):
            continue
        dados = _ler_json(os.path.join(pasta_compras, arq))
        if dados.get("metadata", {}).get("status") != "SUCESSO":
            continue
        url = dados.get("metadata", {}).get("url_consultada", "")
        tipo = _tipo_de_url(url)
        for c in dados.get("respostas", {}).get("resultado", []):
            id_c = str(c.get("idCompra") or c.get("id_compra") or "")
            if id_c:
                tipos.setdefault(id_c, set()).add(tipo)
    return tipos


# ---------------------------------------------------------------------------
# Análise e limpeza
# ---------------------------------------------------------------------------

def limpar(pasta_compras: str, pasta_itens: str, executar: bool) -> None:
    modo = "EXECUTANDO" if executar else "SIMULAÇÃO (dry-run)"
    print(f"\n{'='*60}")
    print(f"  LIMPEZA DE ITENS VAZIOS — {modo}")
    print(f"{'='*60}\n")

    print("📂 Mapeando tipos de compras...")
    tipos_por_id = _mapear_tipos(pasta_compras)
    print(f"   {len(tipos_por_id)} compras mapeadas.\n")

    arquivos = sorted(glob.glob(os.path.join(pasta_itens, "*.json")))
    print(f"📂 {len(arquivos)} arquivos em '{pasta_itens}'\n")

    # Contadores
    apagar:   list[str] = []
    manter:   int = 0
    ignorar:  int = 0   # falhas ou ausentes — não tocamos

    motivos: dict[str, int] = {}

    for caminho in arquivos:
        dados = _ler_json(caminho)
        status = dados.get("metadata", {}).get("status", "")

        # Só analisa SUCESSOs
        if status != "SUCESSO":
            ignorar += 1
            continue

        # Se tem itens → manter sempre
        if _tem_itens(dados):
            manter += 1
            continue

        # Arquivo vazio — verifica se a combinação é inútil
        nome = os.path.basename(caminho)
        partes = nome.replace(".json", "").split("_")
        # formato: itens_{idCompra}_{sufixo}_p{pagina}
        sufixo = partes[-2] if len(partes) >= 3 else ""
        id_c = "_".join(partes[1:-2]) if len(partes) >= 4 else ""

        tipos = tipos_por_id.get(id_c, set())

        # Verifica se ALGUM tipo desse ID justifica remover esse sufixo
        # (todos os tipos do ID devem classificar esse sufixo como inútil)
        if not tipos:
            # ID não encontrado nos arquivos de compras — mantém por segurança
            manter += 1
            continue

        todos_inuteis = all(
            (tipo, sufixo) in COMBINACOES_INUTEIS for tipo in tipos)

        if todos_inuteis:
            motivo = f"{'+'.join(sorted(tipos))} × {sufixo}"
            motivos[motivo] = motivos.get(motivo, 0) + 1
            apagar.append(caminho)
        else:
            manter += 1

    # Relatório
    print(f"  Arquivos com itens (manter) : {manter}")
    print(f"  Falhas/outros (ignorar)     : {ignorar}")
    print(f"  Vazios por combinação inútil: {len(apagar)}\n")

    if motivos:
        print("  Detalhamento por combinação:")
        for motivo, cnt in sorted(motivos.items(), key=lambda x: -x[1]):
            print(f"    {cnt:>6}x  {motivo}")

    if not apagar:
        print("\n✅ Nada a remover.")
        return

    tam_total = sum(os.path.getsize(c) for c in apagar)
    print(
        f"\n  Espaço a liberar: {tam_total / 1024:.0f} KB  ({tam_total / 1024**2:.1f} MB)")

    if not executar:
        print(f"\n⚠️  Dry-run — nenhum arquivo foi apagado.")
        print(f"   Para apagar de verdade: python limpar_itens.py --executar\n")
        return

    print(f"\n🗑️  Apagando {len(apagar)} arquivos...")
    erros = 0
    for caminho in apagar:
        try:
            os.remove(caminho)
        except Exception as e:
            print(f"  ⚠️  Erro ao apagar {caminho}: {e}")
            erros += 1

    apagados = len(apagar) - erros
    print(f"\n✅ Concluído — {apagados} arquivos removidos  |  {erros} erros\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Remove arquivos de itens vazios de combinações tipo×endpoint inúteis."
    )
    parser.add_argument(
        "--executar", action="store_true",
        help="Apaga os arquivos. Sem esta flag roda em dry-run."
    )
    parser.add_argument("--pasta-compras", default=PASTA_COMPRAS)
    parser.add_argument("--pasta-itens",   default=PASTA_ITENS)
    args = parser.parse_args()

    limpar(args.pasta_compras, args.pasta_itens, args.executar)
