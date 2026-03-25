"""
utils/limpar_empenhos_falhos.py
--------------------------------
Remove JSONs com status diferente de SUCESSO das pastas de empenhos,
permitindo que o extrator reprocesse apenas os arquivos com falha.

Pastas verificadas:
  - temp/empenhos/
  - temp/empenhos_itens/
  - temp/empenhos_historico/

Uso:
    python utils/limpar_empenhos_falhos.py
"""

import json
import os

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PASTAS = [
    os.path.join(BASE_DIR, "temp", "empenhos"),
    os.path.join(BASE_DIR, "temp", "empenhos_itens"),
    os.path.join(BASE_DIR, "temp", "empenhos_historico"),
]


# ---------------------------------------------------------------------------
# Lógica principal
# ---------------------------------------------------------------------------

def limpar_falhos(dry_run: bool = False) -> None:
    print("╔════════════════════════════════════════════════════╗")
    print("║         LIMPEZA DE JSONs FALHOS — EMPENHOS         ║")
    print("╚════════════════════════════════════════════════════╝")

    if dry_run:
        print("\n⚠️  MODO DRY-RUN — nenhum arquivo será apagado de verdade.\n")

    total_verificados = 0
    total_apagados    = 0
    total_erros_leitura = 0

    for pasta in PASTAS:
        if not os.path.exists(pasta):
            print(f"\n⚠️  Pasta não encontrada, pulando: {pasta}")
            continue

        arquivos = [a for a in os.listdir(pasta) if a.endswith(".json")]
        apagados_pasta = 0

        print(f"\n📂 {pasta} ({len(arquivos)} arquivos)")

        for nome in arquivos:
            caminho = os.path.join(pasta, nome)
            total_verificados += 1

            try:
                with open(caminho, encoding="utf-8") as f:
                    dados = json.load(f)
                status = dados.get("metadata", {}).get("status", "")
            except Exception as e:
                print(f"  ⚠️  Erro ao ler {nome}: {e}")
                total_erros_leitura += 1
                continue

            if status != "SUCESSO":
                print(f"  🗑️  {'[DRY] ' if dry_run else ''}Apagando [{status or 'SEM STATUS'}] → {nome}")
                if not dry_run:
                    os.remove(caminho)
                apagados_pasta += 1
                total_apagados += 1

        print(f"  ✅ {apagados_pasta} arquivo(s) {'marcados para remoção' if dry_run else 'removidos'} nesta pasta")

    print(f"\n{'='*60}")
    print(f"  Verificados      : {total_verificados}")
    print(f"  {'Seriam removidos' if dry_run else 'Removidos'}       : {total_apagados}")
    print(f"  Erros de leitura : {total_erros_leitura}")
    print(f"{'='*60}")

    if total_apagados == 0:
        print("\n✅ Nenhum arquivo falho encontrado.")
    elif not dry_run:
        print(f"\n✅ Concluído! Rode o extrator para reprocessar os {total_apagados} arquivo(s) removido(s).")


# ---------------------------------------------------------------------------
# Ponto de entrada
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Remove JSONs com status diferente de SUCESSO das pastas de empenhos."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Apenas lista o que seria apagado, sem remover nada.",
    )
    args = parser.parse_args()

    limpar_falhos(dry_run=args.dry_run)