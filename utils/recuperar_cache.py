"""
recuperar_cache_v2.py
---------------------
Script autônomo para recuperar arquivos JSON com falha em comprasv3
buscando o equivalente com sucesso em comprasv2.

Uso:
    python recuperar_cache_v2.py
    python recuperar_cache_v2.py --simular          # dry-run: só mostra o que faria
    python recuperar_cache_v2.py --origem caminho_v2 --destino caminho_v3
"""

import argparse
import json
import os
import shutil
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuração padrão — ajuste se necessário
# ---------------------------------------------------------------------------
PASTA_V2 = r"C:\Users\2213226\Documents\comprasv2\temp\compras"
PASTA_V3 = r"C:\Users\2213226\Documents\comprasv3\temp\compras"


# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------

def ler_status(caminho: Path) -> str:
    """Retorna o status do envelope JSON ('SUCESSO', 'FALHA', ou 'ILEGÍVEL')."""
    try:
        with open(caminho, "r", encoding="utf-8") as f:
            dados = json.load(f)
        return dados.get("metadata", {}).get("status", "SEM_STATUS")
    except Exception:
        return "ILEGÍVEL"


def listar_falhas(pasta: Path) -> list[Path]:
    """Retorna todos os JSONs da pasta cujo status não é SUCESSO."""
    falhas = []
    for arq in sorted(pasta.glob("*.json")):
        if ler_status(arq) != "SUCESSO":
            falhas.append(arq)
    return falhas


# ---------------------------------------------------------------------------
# Motor principal
# ---------------------------------------------------------------------------

def recuperar(pasta_v2: Path, pasta_v3: Path, simular: bool = False) -> None:
    print("=" * 60)
    print("  RECUPERADOR DE CACHE v2 → v3")
    print("=" * 60)
    print(f"  Origem  (v2): {pasta_v2}")
    print(f"  Destino (v3): {pasta_v3}")
    print(
        f"  Modo        : {'SIMULAÇÃO (dry-run)' if simular else 'REAL (copia arquivos)'}")
    print("=" * 60)

    if not pasta_v3.exists():
        print(f"\n❌ Pasta de destino não encontrada: {pasta_v3}")
        return

    if not pasta_v2.exists():
        print(f"\n❌ Pasta de origem não encontrada: {pasta_v2}")
        return

    falhas = listar_falhas(pasta_v3)
    total_falhas = len(falhas)

    if total_falhas == 0:
        print("\n✅ Nenhum arquivo com falha encontrado em v3. Nada a fazer.")
        return

    print(f"\n🔍 {total_falhas} arquivo(s) com falha encontrado(s) em v3.\n")

    copiados = 0
    nao_encontrados = 0
    v2_tambem_falha = 0

    for arq_v3 in falhas:
        nome = arq_v3.name
        arq_v2 = pasta_v2 / nome
        status_v3 = ler_status(arq_v3)

        if not arq_v2.exists():
            print(f"  ⚠️  NÃO ENCONTRADO em v2 : {nome}")
            nao_encontrados += 1
            continue

        status_v2 = ler_status(arq_v2)

        if status_v2 != "SUCESSO":
            print(f"  ⛔ v2 TAMBÉM COM FALHA  : {nome}  (v2={status_v2})")
            v2_tambem_falha += 1
            continue

        if simular:
            print(
                f"  📋 SIMULARIA COPIAR     : {nome}  (v3={status_v3} → v2=SUCESSO)")
        else:
            shutil.copy2(arq_v2, arq_v3)
            print(f"  ✅ COPIADO              : {nome}")

        copiados += 1

    print()
    print("=" * 60)
    print("  RESUMO")
    print("=" * 60)
    print(f"  Falhas em v3            : {total_falhas}")
    print(
        f"  {'Seriam copiados' if simular else 'Copiados de v2'}       : {copiados}")
    print(f"  Não encontrados em v2   : {nao_encontrados}")
    print(f"  v2 também com falha     : {v2_tambem_falha}")
    print("=" * 60)

    if simular and copiados > 0:
        print("\n  ℹ️  Execute sem --simular para aplicar as cópias.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args():
    parser = argparse.ArgumentParser(
        description="Recupera JSONs com falha em v3 copiando o equivalente com sucesso de v2."
    )
    parser.add_argument(
        "--origem",
        default=PASTA_V2,
        help=f"Pasta v2 com os JSONs bons (padrão: {PASTA_V2})",
    )
    parser.add_argument(
        "--destino",
        default=PASTA_V3,
        help=f"Pasta v3 com os JSONs a corrigir (padrão: {PASTA_V3})",
    )
    parser.add_argument(
        "--simular",
        action="store_true",
        help="Dry-run: mostra o que seria feito sem copiar nada.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    recuperar(
        pasta_v2=Path(args.origem),
        pasta_v3=Path(args.destino),
        simular=args.simular,
    )
