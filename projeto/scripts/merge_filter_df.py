# -*- coding: utf-8 -*-
"""
Created on Wed Apr 23 10:00:21 2025

@author: Gabriel
"""
import pandas as pd
import numpy as np
import warnings


def merge_cnpj_prod(cnpj,prod):
    '''
    
    Processa dados de atividade por CNPJ e Município para obter coordenada,
    código de atividade e código de categoria
    - Uma mesma indústria com o mesmo cnpj e lat lon produz produtos diferentes
    - Ou seja, multiplos codigo de categoria e codigo de atividade
    - Por isso, fiz um groupby onde os códigos de categoria e atividade viram uma
    lista para um mesmo CNPJ
    
    Parâmetros: 
        - cnpj, prod = Base de dados de CNPJ e de Produção do ibama, e baixado pelas funções:
            -download_ibama_ctf_data
            -ibama_production_data
            
     Retorna:
         pd.DataFrame: DataFrame com os dados concatenados
         
    '''
    cnpj = cnpj.groupby(['CNPJ', 'Municipio', 'Latitude', 'Longitude']).agg({
        'Codigo da categoria': list,
        'Codigo da atividade': list    
    }).reset_index()
    
    # Fazer o merge com o df_ibama (sem duplicar linhas)
    df_ibama_completo = pd.merge(
        left=cnpj,
        right=prod, 
        left_on=['CNPJ', 'Municipio'],
        right_on=['mv.num_cpf_cnpj', 'mv.nom_municipio'],
        how='right',
        indicator=True
    )
    #print(df_ibama_completo.info())
    not_located = df_ibama_completo['Codigo da atividade'].isna().sum()
    total_ibama_len = len(df_ibama_completo)
    percentage = round((not_located/total_ibama_len) * 100,2)
    print(f'{not_located} CNPJs não localizados {percentage}%')
    
    return df_ibama_completo

''' Esta função não está sendo utilizada.
def filter_activity_category(df, pares_validos, col_atividade='Codigo da atividade', col_categoria='Codigo da categoria'):
    """
    Filtra DataFrame baseado em pares atividade-categoria.
    
    Parâmetros:
        df (pd.DataFrame): DataFrame de entrada
        pares_validos (list): Lista de strings no formato "categoria-atividade"
        col_atividade (str): Nome da coluna de atividades
        col_categoria (str): Nome da coluna de categorias
    
    Retorna:
        pd.DataFrame: DataFrame filtrado
    """
    if col_atividade not in df.columns or col_categoria not in df.columns:
        raise ValueError(f"DataFrame deve conter colunas '{col_atividade}' e '{col_categoria}'")

    # Conjunto com os pares válidos
    pares = set()
    for par in pares_validos:
        try:
            cat, atv = map(str.strip, par.split('-'))
            if atv and cat:
                pares.add((int(cat), int(atv)))  # Convertendo para int se os dados forem números
        except (ValueError, AttributeError):
            continue

    # Geração da máscara
    mascara = []
    for _, row in df.iterrows():
        atividades = row[col_atividade]
        categorias = row[col_categoria]

        # Garante que sejam listas
        if not isinstance(atividades, list) or not isinstance(categorias, list):
            mascara.append(False)
            continue

        match = any((c, a) in pares for c, a in zip(categorias, atividades))
        mascara.append(match)

    return df[pd.Series(mascara, index=df.index)]
'''

def conecta_ibama_ef(df_ibama, df_ef, df_conector):
    # Garantir que os códigos estejam no mesmo tipo
    df_conector['PRODLIST'] = df_conector['PRODLIST'].astype(str)
    df_ibama['cod_produto'] = df_ibama['cod_produto'].astype(str)

    # Mesclar df_ibama com o conector via código do produto
    df_merged = df_ibama.merge(
        df_conector[['PRODLIST', 'NFR', 'Table']],
        left_on='cod_produto',
        right_on='PRODLIST',
        how='left'
    )

    # Padronizar tipo da coluna 'Table'
    df_merged['Table'] = df_merged['Table'].astype(str)

    # Realizar o merge com a base ef (EEA)
    df_final = df_merged.merge(
        df_ef,
        on=['NFR', 'Table'],
        how='left'
    )

    # Remover coluna auxiliar
    df_final = df_final.drop(columns=['PRODLIST'])

    return df_final


def converter_para_hl(df_conversao, qtd_produzida, unidade_medida, cod_produto=None):
    """
    Converte uma quantidade para hectolitros (hL) baseado nas regras do CSV.
    
    Parâmetros:
    - qtd_produzida: quantidade a ser convertida
    - unidade_medida: unidade de medida original
    - cod_produto: código do produto (opcional, para regras específicas)
    
    Retorna:
    - Quantidade convertida em hL ou np.nan se não encontrar conversão
    """
    # Primeiro tenta encontrar por código específico do produto
    if cod_produto is not None:
        # Verifica se o código começa com algum valor específico no CSV
        mascara_cod = df_conversao['cod_produto'].astype(str).str.startswith(str(cod_produto))
        mascara_unidade = df_conversao['unidade'] == unidade_medida
        resultado_especifico = df_conversao[mascara_cod & mascara_unidade]
        
        if not resultado_especifico.empty:
            return qtd_produzida * resultado_especifico.iloc[0]['hl']
    
    # Se não encontrou específico, procura nas regras gerais
    mascara_geral = (df_conversao['cod_produto'] == 'geral') & (df_conversao['unidade'] == unidade_medida)
    resultado_geral = df_conversao[mascara_geral]
    
    if not resultado_geral.empty:
        return qtd_produzida * resultado_geral.iloc[0]['hl']
    
    # Se não encontrou em nenhum lugar, retorna NaN
    return pd.NA

