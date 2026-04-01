import os
import json
import shutil
from pathlib import Path

def organizar_arquivos_vazios():
    # Caminho base: temp está no mesmo nível de utils
    base_path = Path(__file__).parent.parent / "temp"
    base_path = base_path.resolve()
    
    if not base_path.exists():
        print(f"❌ Erro: Pasta 'temp' não encontrada em {base_path}")
        return

    print(f"🧹 Iniciando limpeza de arquivos vazios em: {base_path}\n")

    for pasta in os.listdir(base_path):
        pasta_path = base_path / pasta
        
        if pasta_path.is_dir() and pasta != "vazios":
            # Cria a subpasta 'vazios' dentro de cada categoria
            pasta_vazios = pasta_path / "vazios"
            pasta_vazios.mkdir(exist_ok=True)
            
            arquivos_movidos = 0
            # Lista apenas arquivos .json
            arquivos = [f for f in os.listdir(pasta_path) if f.endswith('.json')]
            
            for nome_arq in arquivos:
                caminho_arq = pasta_path / nome_arq
                is_vazio = False
                
                try:
                    # 1. Abre, lê e identifica se está vazio
                    with open(caminho_arq, 'r', encoding='utf-8') as f:
                        dados = json.load(f)
                        
                        if isinstance(dados, dict):
                            respostas = dados.get('respostas', {})
                            if isinstance(respostas, dict):
                                if respostas.get('resultado') == []:
                                    is_vazio = True
                            elif isinstance(respostas, list) and not respostas:
                                is_vazio = True
                        elif isinstance(dados, list) and not dados:
                            is_vazio = True
                    
                    # 2. Fora do bloco 'with', o arquivo já está fechado.
                    # Agora o Windows permite a movimentação.
                    if is_vazio:
                        destino = pasta_vazios / nome_arq
                        shutil.move(str(caminho_arq), str(destino))
                        arquivos_movidos += 1
                            
                except Exception as e:
                    print(f"  ⚠️ Erro ao processar {nome_arq}: {e}")

            if arquivos_movidos > 0:
                print(f"✅ [{pasta}] {arquivos_movidos} arquivos vazios movidos para {pasta}/vazios/")
            else:
                print(f"ℹ️ [{pasta}] Nenhum arquivo vazio encontrado.")

    print("\n✨ Faxina concluída com sucesso!")

if __name__ == "__main__":
    organizar_arquivos_vazios()