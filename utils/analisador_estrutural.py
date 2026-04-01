import os
import json
import pandas as pd
from collections import Counter
from pathlib import Path

def analisar_data_lake_temp():
    # Define o caminho da pasta temp (um nível acima da utils)
    base_path = Path(__file__).parent.parent / "temp"
    base_path = base_path.resolve()
    
    print(f"🔍 Iniciando auditoria detalhada em: {base_path}\n")
    
    relatorio = []
    
    if not base_path.exists():
        print(f"❌ Erro: Pasta 'temp' não encontrada em {base_path}")
        return

    for pasta in os.listdir(base_path):
        pasta_path = base_path / pasta
        
        if pasta_path.is_dir():
            print(f"📂 Analisando pasta: {pasta}...")
            
            stats = {
                "total": 0,
                "tamanho": 0,
                "erros": 0,
                "vazios": 0,
                "chaves": Counter()
            }
            
            for arq in os.listdir(pasta_path):
                if arq.endswith('.json'):
                    stats["total"] += 1
                    file_path = pasta_path / arq
                    stats["tamanho"] += file_path.stat().st_size
                    
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            dados = json.load(f)
                            
                            # --- Lógica de Detecção de Resultado Vazio ---
                            is_vazio = False
                            
                            # Caso 1: Estrutura respostas -> resultado: []
                            if isinstance(dados, dict):
                                respostas = dados.get('respostas', {})
                                if isinstance(respostas, dict):
                                    resultado = respostas.get('resultado')
                                    if resultado == []: is_vazio = True
                                elif isinstance(respostas, list) and len(respostas) == 0:
                                    is_vazio = True
                                    
                            # Caso 2: Estrutura raiz é uma lista vazia []
                            elif isinstance(dados, list) and len(dados) == 0:
                                is_vazio = True
                                
                            if is_vazio:
                                stats["vazios"] += 1
                            # ---------------------------------------------
                            
                            # Mapeamento de colunas (apenas se não for vazio)
                            else:
                                exemplo = {}
                                if isinstance(dados, dict):
                                    respostas = dados.get('respostas', {})
                                    if isinstance(respostas, dict):
                                        res = respostas.get('resultado', [])
                                        exemplo = res[0] if isinstance(res, list) and res else res
                                    else:
                                        exemplo = respostas[0] if isinstance(respostas, list) and respostas else dados
                                
                                if isinstance(exemplo, dict):
                                    stats["chaves"].update(exemplo.keys())
                                
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        stats["erros"] += 1

            relatorio.append({
                "Pasta": pasta,
                "Qtd Arq": stats["total"],
                "Vazios": stats["vazios"],
                "Tamanho (MB)": round(stats["tamanho"] / (1024 * 1024), 2),
                "Erros": stats["erros"],
                "Colunas": ", ".join(list(stats["chaves"].keys())[:5])
            })

    df = pd.DataFrame(relatorio)
    print("\n" + "="*90)
    print("📊 RESUMO DA ESTRUTURA TEMP (INCLUINDO VAZIOS)")
    print("="*90)
    print(df.to_string(index=False))
    print("="*90)

if __name__ == "__main__":
    analisar_data_lake_temp()