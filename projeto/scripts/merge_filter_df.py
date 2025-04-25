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
