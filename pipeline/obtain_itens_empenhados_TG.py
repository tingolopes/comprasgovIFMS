import pandas as pd

def obter_itens_empenhados_TG():
    url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vS_ctP42jBDaNS5piEuXU_VBkDxpiGA-_97Fbr03SqhdsANb8sLldukB4bkAwGo4sAr2ttvrPWE5Kvx/pub?output=xlsx'
    
    # 1. Lê o arquivo Excel da URL
    df = pd.read_excel(url)
    
    # 2. Dividir Colunas e Limpar (Split + Replace)
    # Pegamos o que vem antes do " - " e removemos "Item compra: "
    df['NE Item.1'] = df['NE Item'].str.split(' - ').str[0].str.replace('Item compra: ', '', regex=False)
    
    # NE - Informação Complementar (Split para pegar a primeira parte)
    df['Complementar.1'] = df['NE - Informação Complementar'].astype(str).str.split(' - ').str[0]
    
    # 3. Criar ID Mesclado (Colunas Mescladas)
    # Convertemos para string para garantir a concatenação
    df['id_compra_item_unidade_favorecido_doc'] = (
        df['Complementar.1'].astype(str) + 
        df['NE Item.1'].astype(str) + 
        df['Emitente - UG'].astype(str) + 
        df['Favorecido Doc. Número'].astype(str)
    )
    
    # 4. Lógica de Quantidade Ajustada (Personalização Adicionada)
    # Se operação for ANULACAO ou CANCELAMENTO, inverte o sinal
    filtro_anulacao = df['NE Item - Operação'].isin(['ANULACAO', 'CANCELAMENTO'])
    df['qtd_ajustada'] = df['NE Item - Qtde Operação'].where(~filtro_anulacao, -df['NE Item - Qtde Operação'])

    # 5. Renomear e Selecionar Colunas (Colunas Renomeadas + Reordenadas)
    mapa_colunas = {
        "NE": "empenho",
        "Favorecido Doc. Nome": "favorecido_nome",
        "NE Item - Operação": "operacao",
        "NE - Dia Emissão": "emissao",
        "NE - Natureza Despesa": "grupo_despesa",
        "NE Item - Valor Unit. Operação": "vlr_unitario"
    }
    
    df = df.rename(columns=mapa_colunas)
    
    # 6. Seleção Final (Colunas Reordenadas + Removidas)
    colunas_finais = [
        "empenho", "favorecido_nome", "operacao", "emissao", 
        "id_compra_item_unidade_favorecido_doc", "grupo_despesa", 
        "qtd_ajustada", "vlr_unitario"
    ]
    
    df_finais = df[colunas_finais]
    
    # Salva o arquivo limpo
    df_finais.to_excel('data/itens_empenhados_TG.xlsx', index=False)
    
    return df_finais

if __name__ == "__main__":
    obter_itens_empenhados_TG()