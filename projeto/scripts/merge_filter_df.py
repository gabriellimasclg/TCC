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
    
    Parâmetros: 
        - cnpj, prod = Base de dados de CNPJ e de Produção do ibama, e baixado pelas funções:
            -download_ibama_ctf_data
            -ibama_production_data
            
     Retorna:
         pd.DataFrame: DataFrame com os dados concatenados
         
    '''
    
    # Uma mesma indústria com o mesmo cnpj e lat lon produz produtos diferentes
    # Ou seja, multiplos codigo de categoria e codigo de atividade
    # Por isso, fiz um groupby onde os códigos de categoria e atividade viram uma lista para um mesmo CNPJ
    # acabei não utilizando estes códigos, mas mantive por segurança
    cnpj = cnpj.groupby(['CNPJ', 'MUNICIPIO', 'LATITUDE', 'LONGITUDE','ESTADO']).agg({
        'CODIGO DA CATEGORIA': list,
        'CODIGO DA ATIVIDADE': list    
    }).reset_index()
    
    # Fazer o merge com o df_ibama (sem duplicar linhas)
    df_ibama_completo = pd.merge(
        left=cnpj, #tabela unida a esqueda
        right=prod, #tabela unida a direita
        left_on=['CNPJ', 'MUNICIPIO'], #chaves da tabela a esqueda
        right_on=['mv.num_cpf_cnpj', 'mv.nom_municipio'], #chaves da tabela a direita
        how='right', # todas as linhas da tabela da direita (prod) serão mantidas.
        indicator=True #informa a condição da mesclagem (right_only - não estava em CNPJ)
    )
    
    return df_ibama_completo


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

