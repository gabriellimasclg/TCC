# -*- coding: utf-8 -*-
"""
Created on Wed Apr 23 09:01:35 2025

@author: Gabriel
"""
import os
import pandas as pd
from clean_text import clean_text

def ibama_production_data(repo_path):
    """
    Processa dados de produção do IBAMA a partir de arquivos XLSX, limpando e
    consolidando os dados.
    
    Parâmetros:
        repo_path (str): Caminho raiz do repositório onde estão as pastas inputs
        /outputs
    
    Retorna:
        pd.DataFrame: DataFrame com os dados processados
        str: Caminho do arquivo CSV salvo
    """
    # Validação do caminho
    if not os.path.exists(repo_path):
        raise ValueError(f"Caminho não encontradoS: {repo_path}")
    
    # Define caminhos
    raw_dir = os.path.join(repo_path, 'inputs', 'dadosIbamaProd', 'raw')
    processed_dir = os.path.join(repo_path, 'outputs', 'dadosIbamaProd', 'processed')
    
    # Garante que as pastas existam
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    
    try:
        # Caminho completo do arquivo
        file_path = os.path.join(raw_dir, 'RAPP.xlsx')
        
        # Lê as duas abas do Excel
        aba1 = pd.read_excel(file_path, sheet_name=0, dtype={'mv.num_cpf_cnpj': str})
        aba2 = pd.read_excel(file_path, sheet_name=1, dtype={'mv.num_cpf_cnpj': str})
        
        # Combina as abas
        df_ibama_prod = pd.concat([aba1, aba2], ignore_index=True)
        
        # Limpeza dos dados
        df_ibama_clean = df_ibama_prod.copy()
        
        #limpeza geral
        df_ibama_clean['mv.nom_municipio'] = df_ibama_clean['mv.nom_municipio'].apply(clean_text)
        df_ibama_clean['mv.nom_pessoa'] = df_ibama_clean['mv.nom_pessoa'].apply(clean_text)
        
        # Salva o resultado
        output_file = os.path.join(processed_dir, 'PRODUCAO_IBAMA_CONSOLIDADO.csv')
        df_ibama_clean.to_csv(output_file, index=False, encoding='utf-8')
        
        return df_ibama_clean
        
    except FileNotFoundError:
        raise FileNotFoundError(f"Arquivo RAPP.xlsx não encontrado em {raw_dir}")
    except Exception as e:
        raise Exception(f"Erro ao processar dados: {str(e)}")
        
        
def import_products_code(repo_path):
    '''
    Verificar online: https://servicos.ibama.gov.br/ctfcd/manual/html/lista_produtos.htm
    Baixar excel: https://www.ibge.gov.br/estatisticas/metodos-e-classificacoes/classificacoes-e-listas-estatisticas/9153-lista-de-produtos-da-industria.html
    '''
    # Caminho do arquivo
    csv_path = os.path.join(repo_path, 'inputs', 'cod_produto.csv')

    # Lê o CSV com header na linha 1 (índice 1)
    cod_produto = pd.read_csv(csv_path, encoding='latin1', header=2, dtype={'PRODLIST': str})
    # Remove linhas com 'PRODLIST' ou NaN na coluna 'PRODLIST'
    cod_produto = cod_produto[~cod_produto['PRODLIST'].isin(['PRODLIST'])]
    cod_produto = cod_produto[~cod_produto['PRODLIST'].astype(str).str.startswith('CNAE')]
    cod_produto = cod_produto.dropna(subset=['PRODLIST'])

    # Reinicia o índice
    cod_produto.reset_index(drop=True, inplace=True)
    return cod_produto
