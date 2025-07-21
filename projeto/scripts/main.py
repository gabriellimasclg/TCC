# -*- coding: utf-8 -*-
"""
Created on Mon Jul 21 09:47:06 2025

@author: glima
"""

#%%=========================== Bibliotecas ==================================
import os
import numpy as np
import pandas as pd
from download_database import download_ibama_ctf_data
from import_database import ibama_production_data, import_products_code
from merge_filter_df import merge_cnpj_prod, conecta_ibama_ef, converter_para_hl

#%%===========================Criação da Base Geral============================

# Caminho da pasta do projeto
repo_path = os.path.dirname(os.getcwd())

# Faz o downloado da base de dados com CNPJ + Coordenadas
df_ibama_cnpj = download_ibama_ctf_data(repo_path)

# Importa DF com Dados de produção com CNPJ + Código de Produto + Produção
df_ibama_prod = ibama_production_data(repo_path) 



# DF com Produção + Código de produto + Coordenadas
df_ibama = merge_cnpj_prod(df_ibama_cnpj,df_ibama_prod) #mesclar p obter coordenadas e código de atividade

'''
FUTURO: Fazer função de verificação/Status da mesclagem
    VERIFICAÇÃO SE TODOS OS MUNICÍPIOS DE PROD ESTÃO EM NO DF_IBAMA_CNPJ 
    # Municípios únicos em cada base
    municipios_prod = set(df_ibama_prod['mv.nom_municipio'].unique())
    municipios_cnpj = set(df_ibama_cnpj['MUNICIPIO'].unique())
    
    # Presentes em ambas
    presentes_ambas = municipios_prod & municipios_cnpj
    
    # Presentes apenas na primeira
    apenas_prod = municipios_prod - municipios_cnpj
    
    # Presentes apenas na segunda
    apenas_cnpj = municipios_cnpj - municipios_prod
'''

#Base de dados com todos os Códigos de Produto
cod_produto= import_products_code(repo_path)

#Base de dados com Alimentos apenas >> pega cod_produto >> filtra códigos dos alimentos >> exporta
'''FAZENDO'''

# A base de dados do cod_produto foi exportada para OutPuts >> CodProdutoParaClassificar
# Copiar e colar em Imputs\MaterialGeradoManualmente
# Adicionar Colunas OBSERVAÇÕES; NFR; TABLE
# Classificar em Relação ao NFR manualmente, com JUSTIFICATIVAS em casos de aproximações
# Renomear para CodProdutoClassificadoNFR


