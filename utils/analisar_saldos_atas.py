import os
import json
import csv
from datetime import datetime

# ==============================================================================
# CONFIGURAÇÃO
# ==============================================================================

PASTA_ATAS_SALDOS = os.path.join(os.path.dirname(
    os.path.abspath(__file__)), "..", "temp", "atas_saldos")


# ==============================================================================
# UTILITÁRIOS
# ==============================================================================

def extrair_numero_ata_do_arquivo(nome_arquivo):
    """Extrai o número da ata a partir do nome do arquivo JSON."""
    return nome_arquivo.replace(".json", "")


def extrair_numero_ata_da_url(url):
    """Tenta extrair o número da ata a partir da URL consultada nos metadados."""
    try:
        parte = url.split("numeroAta=")[1]
        numero = parte.split("&")[0]
        return numero.replace("%2F", "/")
    except Exception:
        return ""


# ==============================================================================
# ANÁLISE PRINCIPAL
# ==============================================================================

def analisar_saldos_estourados():
    print("╔══════════════════════════════════════════════════════════╗")
    print("║       ANÁLISE DE SALDOS ESTOURADOS — ATAS/ITENS          ║")
    print("╚══════════════════════════════════════════════════════════╝")

    if not os.path.exists(PASTA_ATAS_SALDOS):
        print(f"\n❌ Pasta não encontrada: {PASTA_ATAS_SALDOS}")
        return

    arquivos = [a for a in os.listdir(
        PASTA_ATAS_SALDOS) if a.endswith(".json")]
    print(f"\n📂 {len(arquivos)} arquivos encontrados em '{PASTA_ATAS_SALDOS}'")

    ocorrencias = []
    total_arquivos_lidos = 0
    total_itens_analisados = 0

    for nome_arquivo in sorted(arquivos):
        caminho = os.path.join(PASTA_ATAS_SALDOS, nome_arquivo)
        try:
            with open(caminho, "r", encoding="utf-8") as f:
                dados = json.load(f)

            # Extrai número da ata — tenta URL nos metadados, fallback para nome do arquivo
            url = dados.get("metadata", {}).get("url_consultada", "")
            numero_ata = extrair_numero_ata_da_url(
                url) or extrair_numero_ata_do_arquivo(nome_arquivo)

            itens = dados.get("respostas", {}).get("resultado", [])
            if isinstance(itens, dict):
                itens = [itens]

            total_arquivos_lidos += 1
            total_itens_analisados += len(itens)

            for item in itens:
                qtd_registrada = item.get("quantidadeRegistrada") or 0
                qtd_empenhada = item.get("quantidadeEmpenhada") or 0

                if qtd_empenhada > qtd_registrada:
                    ocorrencias.append({
                        "numero_ata": numero_ata,
                        "numero_item": item.get("numeroItem", ""),
                        "unidade": item.get("unidade", ""),
                        "tipo": item.get("tipo", ""),
                        "quantidade_registrada": qtd_registrada,
                        "quantidade_empenhada": qtd_empenhada,
                        "saldo_empenho": item.get("saldoEmpenho", ""),
                        "data_atualizacao": item.get("dataHoraAtualizacao", "") or "",
                        "url_api": url,
                    })

        except Exception as e:
            print(f"  ⚠️  Erro ao ler '{nome_arquivo}': {e}")
            continue

    # Exibe resumo no terminal
    print(f"\n📊 Arquivos lidos:      {total_arquivos_lidos}")
    print(f"   Itens analisados:    {total_itens_analisados}")
    print(f"   Ocorrências encontradas: {len(ocorrencias)}")

    if not ocorrencias:
        print("\n✅ Nenhum item com quantidadeEmpenhada > quantidadeRegistrada encontrado.")
        return

    # Exibe ocorrências no terminal
    print(f"\n{'='*60}")
    print(f"  ITENS COM SALDO ESTOURADO")
    print(f"{'='*60}")
    for o in ocorrencias:
        print(f"\n  Ata:     {o['numero_ata']}  |  Item: {o['numero_item']}")
        print(f"  Unidade: {o['unidade']}")
        print(
            f"  Qtd Registrada: {o['quantidade_registrada']}  |  Qtd Empenhada: {o['quantidade_empenhada']}  |  Saldo: {o['saldo_empenho']}")
        print(f"  Atualizado em: {o['data_atualizacao']}")
        print(f"  URL API: {o['url_api']}")

    print(f"\n🏁 Análise concluída em: {datetime.now().strftime('%H:%M:%S')}")


# ==============================================================================
# PONTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    analisar_saldos_estourados()
