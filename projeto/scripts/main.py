# -*- coding: utf-8 -*-
"""
Created on Mon Jul 21 09:47:06 2025

@author: glima
"""

#%%============================ Bibliotecas====================================
import os
import numpy as np
import pandas as pd
from download_database import download_ibama_ctf_data
from import_database import ibama_production_data, import_treat_export_food_code
from merge_filter_df import merge_cnpj_prod, conecta_ibama_ef, converter_para_hl, conecta_ibama_ef_debug
from cnpj_analisys import CNPJAnalysis

#%%===========================Criação da Base Geral============================

# Caminho da pasta do projeto
repo_path = os.path.dirname(os.getcwd())

'''#parte do código que cria todas as pastas necessárias'''

# Faz o downloado da base de dados com CNPJ + Coordenadas
df_ibama_cnpj = download_ibama_ctf_data(repo_path)

# Importa DF com Dados de produção com CNPJ + Código de Produto + Produção
df_ibama_prod = ibama_production_data(repo_path) 

# DF com Produção + Código de produto + Coordenadas
df_ibama = merge_cnpj_prod(df_ibama_cnpj,df_ibama_prod) #mesclar p obter coordenadas e código de atividade

#contabiliza quantos são CPF (11 dígitos) e quantos são CNPJ (14 dígitos)
CNPJAnalysis(df_ibama)

#Remover CPFs (desconsiderável)
df_ibama = df_ibama[df_ibama['mv.num_cpf_cnpj'].str.len() == 14]

#%%============== Base com NFR + Código do Produto + Produção =================

#Base de dados com todos os Códigos de Produto
cod_produto= import_treat_export_food_code(repo_path)

#vou filtrar os códigos de produto de interesse e exportar (itens que se enquadram no 2.h.2 no nfr)
prefixos = ('10', '11')
cod_produto_interesse = cod_produto[
    cod_produto['PRODLIST'].astype(str).str.startswith(prefixos)
]

cod_produto_interesse.to_excel(os.path.join(repo_path, 'outputs','CodProdutoParaClassificar.xlsx'), index=False)

#Exportei manualmente para a pasta inputs > material gerado manualmente
CodProdutoClassificadoNFR = pd.read_excel(os.path.join(repo_path,'inputs','MaterialGeradoManualmente','CodProdutoClassificadoNFR.xlsx'),
                                          dtype={'PRODLIST': str})

#filtrei os alimentos que tem emissões a serem consideradas
CodProdutoClassificadoNFR = CodProdutoClassificadoNFR[CodProdutoClassificadoNFR['EmissaoNMCOV_NFR'] != 'Não']

# Base de dados dos fatores de emissão tier 2
eea_ef = pd.read_csv(os.path.join(repo_path, 'inputs','MaterialBaixado', 'EF_tier2.csv'))

# Conexão das bases de dados de apenas os classificados como emissores de NMCOV
df_ibama_EF = conecta_ibama_ef(df_ibama,eea_ef,CodProdutoClassificadoNFR)

#Filtrar apenas os produtos com emissão
df_ibama_EF = df_ibama_EF[df_ibama_EF['Table'].notna()]

#%%=============== Ajuste das unidades para calcular emissão ==================






#Base de dados com Alimentos apenas >> pega cod_produto >> filtra códigos dos alimentos >> exporta
'''FAZENDO'''

# A base de dados do cod_produto foi exportada para OutPuts >> CodProdutoParaClassificar
# Copiar e colar em Imputs\MaterialGeradoManualmente
# Adicionar Colunas OBSERVAÇÕES; NFR; TABLE
# Classificar em Relação ao NFR manualmente, com JUSTIFICATIVAS em casos de aproximações
# Renomear para CodProdutoClassificadoNFR


