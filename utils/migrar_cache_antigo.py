"""
migrar_cache_antigo.py
-----------------------
Migra JSONs do cache antigo (temp_atas_saldos_id / temp_atas_unidades_id)
para as pastas do novo pipeline (temp/atas_saldos / temp/atas_unidades).

O nome do arquivo novo é derivado da URL gravada no metadata de cada JSON,
garantindo que o arquivo chegue com o nome que o extrator espera encontrar.

Uso:
    python migrar_cache_antigo.py

Ou sobrescrevendo as pastas padrão:
    python migrar_cache_antigo.py \\
        --pasta_saldos_antigo   "C:/caminho/temp_atas_saldos_id" \\
        --pasta_unidades_antigo "C:/caminho/temp_atas_unidades_id"
"""

import argparse
import json
import os
import shutil
import sys
from urllib.parse import parse_qs, urlparse

# ---------------------------------------------------------------------------
# Pastas padrão (ajuste se necessário)
# ---------------------------------------------------------------------------
PASTA_SALDOS_ANTIGO = r"C:\Users\2213226\Documents\comprasv2\temp\temp_atas_saldos_id"
PASTA_UNIDADES_ANTIGO = r"C:\Users\2213226\Documents\comprasv2\temp\temp_atas_unidades_id"

# Garante que a raiz do projeto está no path,
# independente de o script estar em utils/ ou na raiz
_RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _RAIZ)
from config.config import CONFIG_ATAS  # noqa: E402  # isort: skip

PASTA_SALDOS_NOVO = CONFIG_ATAS["pasta_cache_saldos"]    # temp/atas_saldos
PASTA_UNIDADES_NOVO = CONFIG_ATAS["pasta_cache_unidades"]  # temp/atas_unidades

# Sigla da UASG gerenciadora (usada no nome do arquivo)
SIGLA = CONFIG_ATAS["uasg"]["sigla"]


# ---------------------------------------------------------------------------
# Derivação do nome do arquivo novo a partir da URL do metadata
# ---------------------------------------------------------------------------

def _nome_saldo(url: str) -> str | None:
    """
    Deriva nome do arquivo de saldo a partir da URL consultada.
    Ex: ...4_consultarEmpenhosSaldoItem?numeroAta=00001%2F2023&...&pagina=1
      → atas_saldos_RT_00001_2023_p1.json
    """
    try:
        qs = parse_qs(urlparse(url).query)
        numero_ata = qs.get("numeroAta", [""])[0]   # "00001/2023"
        pagina = qs.get("pagina",    ["1"])[0]
        if not numero_ata:
            return None
        slug = numero_ata.replace("/", "_")          # "00001_2023"
        return f"atas_saldos_{SIGLA}_{slug}_p{pagina}.json"
    except Exception:
        return None


def _nome_unidade(url: str) -> str | None:
    """
    Deriva nome do arquivo de unidade a partir da URL consultada.
    Ex: ...3_consultarUnidadesItem?numeroAta=00001%2F2023&...&numeroItem=00001&pagina=1
      → atas_unidades_RT_00001_2023_00001_p1.json
    """
    try:
        qs = parse_qs(urlparse(url).query)
        numero_ata = qs.get("numeroAta",   [""])[0]
        num_item = qs.get("numeroItem",  [""])[0]
        pagina = qs.get("pagina",      ["1"])[0]
        if not numero_ata or not num_item:
            return None
        slug = numero_ata.replace("/", "_")
        return f"atas_unidades_{SIGLA}_{slug}_{num_item}_p{pagina}.json"
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Motor de migração
# ---------------------------------------------------------------------------

def _migrar_pasta(pasta_origem: str, pasta_destino: str,
                  fn_nome, label: str) -> tuple[int, int, int]:
    """
    Varre pasta_origem, deriva o nome correto de cada JSON e copia
    para pasta_destino. Nunca sobrescreve um SUCESSO existente.

    Retorna (copiados, pulados, erros).
    """
    if not os.path.exists(pasta_origem):
        print(f"  ⚠️  Pasta não encontrada: {pasta_origem}")
        return 0, 0, 0

    os.makedirs(pasta_destino, exist_ok=True)

    arquivos = [f for f in os.listdir(pasta_origem) if f.endswith(".json")]
    print(f"\n📂 {label}")
    print(f"   Origem  : {pasta_origem}")
    print(f"   Destino : {pasta_destino}")
    print(f"   Arquivos encontrados: {len(arquivos)}")

    copiados = pulados = erros = ja_existia = 0

    for arq in sorted(arquivos):
        caminho_src = os.path.join(pasta_origem, arq)

        # Lê o JSON e valida
        try:
            with open(caminho_src, encoding="utf-8") as f:
                dados = json.load(f)
        except Exception as exc:
            print(f"   ❌ Erro ao ler {arq}: {exc}")
            erros += 1
            continue

        meta = dados.get("metadata", {})
        status = meta.get("status", "")
        url = meta.get("url_consultada", "")

        # Só migra SUCESSOs
        if status != "SUCESSO":
            print(f"   ⏭️  Ignorado (status={status}): {arq}")
            pulados += 1
            continue

        # Deriva o nome do arquivo destino a partir da URL
        nome_novo = fn_nome(url)
        if not nome_novo:
            print(f"   ⚠️  Não foi possível derivar nome para: {arq}")
            print(f"        URL: {url[:80]}")
            erros += 1
            continue

        caminho_dst = os.path.join(pasta_destino, nome_novo)

        # Não sobrescreve SUCESSO existente
        if os.path.exists(caminho_dst):
            try:
                with open(caminho_dst, encoding="utf-8") as f:
                    existente = json.load(f)
                if existente.get("metadata", {}).get("status") == "SUCESSO":
                    ja_existia += 1
                    continue
            except Exception:
                pass

        shutil.copy2(caminho_src, caminho_dst)
        copiados += 1

    print(f"   ✅ Copiados  : {copiados}")
    print(f"   ⏭️  Já existiam: {ja_existia}")
    print(f"   ⏭️  Ignorados : {pulados}")
    if erros:
        print(f"   ❌ Erros     : {erros}")

    return copiados, pulados, erros


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def migrar(pasta_saldos_antigo: str = PASTA_SALDOS_ANTIGO,
           pasta_unidades_antigo: str = PASTA_UNIDADES_ANTIGO) -> None:

    print("=" * 60)
    print("🔄 MIGRAÇÃO DE CACHE ANTIGO → NOVO PIPELINE")
    print(f"   Sigla UASG: {SIGLA}")
    print("=" * 60)

    c1, p1, e1 = _migrar_pasta(
        pasta_saldos_antigo,
        PASTA_SALDOS_NOVO,
        _nome_saldo,
        "SALDOS (4_consultarEmpenhosSaldoItem)",
    )

    c2, p2, e2 = _migrar_pasta(
        pasta_unidades_antigo,
        PASTA_UNIDADES_NOVO,
        _nome_unidade,
        "UNIDADES (3_consultarUnidadesItem)",
    )

    print("\n" + "=" * 60)
    print("📊 RESUMO FINAL")
    print(f"   Saldos   copiados : {c1}")
    print(f"   Unidades copiadas : {c2}")
    total_erros = e1 + e2
    if total_erros:
        print(f"   Erros            : {total_erros}")
        print("\n⚠️  Migração concluída com erros.")
    else:
        print("\n✅ Migração concluída com sucesso!")
    print("=" * 60)
    print("\nPróximo passo: python main.py --modo transformer_atas_saldos")
    print("               python main.py --modo transformer_atas_unidades")


def _limpar_pycache() -> None:
    raiz = os.path.dirname(os.path.abspath(__file__))
    for dirpath, dirnames, _ in os.walk(raiz):
        for d in dirnames:
            if d == "__pycache__":
                shutil.rmtree(os.path.join(dirpath, d), ignore_errors=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Migra cache antigo de saldos/unidades para o novo pipeline."
    )
    parser.add_argument(
        "--pasta_saldos_antigo",
        default=PASTA_SALDOS_ANTIGO,
        help="Pasta origem dos JSONs de saldo (padrão: temp_atas_saldos_id)",
    )
    parser.add_argument(
        "--pasta_unidades_antigo",
        default=PASTA_UNIDADES_ANTIGO,
        help="Pasta origem dos JSONs de unidades (padrão: temp_atas_unidades_id)",
    )
    args = parser.parse_args()
    migrar(args.pasta_saldos_antigo, args.pasta_unidades_antigo)

    _limpar_pycache()
