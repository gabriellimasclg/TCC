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
from import_database import ibama_production_data_v1,ibama_production_data_v2, import_treat_export_food_code
from merge_filter_df import agrupar_e_somar_dados, merge_cnpj_prod, conecta_ibama_ef
from cnpj_analisys import CNPJAnalysis
from tratamentoOutliers import tratamento_outliers

#%%===========================Criação da Base Geral============================

# Caminho da pasta do projeto
repo_path = os.path.dirname(os.getcwd())

'''#parte do código que cria todas as pastas necessárias'''

# Faz o downloado da base de dados com CNPJ + Coordenadas
df_ibama_cnpj = download_ibama_ctf_data(repo_path)

# Importa DF com Dados de produção com CNPJ + Código de Produto + Produção
df_ibama_prod_v1 = ibama_production_data_v1(repo_path) 
df_ibama_prod_v2 = ibama_production_data_v2(repo_path) 
df_ibama_prod = pd.concat([df_ibama_prod_v1,df_ibama_prod_v2])
df_ibama_prod = agrupar_e_somar_dados(df_ibama_prod)

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

#exportei para classificar manualmente
cod_produto_interesse.to_excel(os.path.join(repo_path, 'outputs','CodProdutoParaClassificar.xlsx'), index=False)

#Exportei manualmente para a pasta inputs

#Importei material gerado manualmente
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

# fazer um unique; Sendo Unit a unidade desejada e os outros referentes ao RAPP
# Criei contagem para ter mais segurança ao descartar alguma unidade
# (vou colocar fator zero nesses)

# Define as colunas para agrupar
colunas_agrupamento = ['cod_produto', 'nom_produto', 'unidade_medida', 'sig_unidmed', 'Unit']

# Agrupa pelas colunas e conta o tamanho de cada grupo
df_unidades_bruto = df_ibama_EF.groupby(colunas_agrupamento).size().reset_index(name='contagem')

#exportei para colocar os fatores de conversão manualmente
df_unidades_bruto.to_excel(os.path.join(repo_path, 'outputs','UnidadesFatorConversãoBruto.xlsx'), index=False)

#Importar base com fatores de conversão
df_unidades = pd.read_excel(os.path.join(repo_path, 'inputs','MaterialGeradoManualmente', 'UnidadesFatorConversão.xlsx'),
                            dtype={'cod_produto': str})

#%%===========Geração do DF de produção bruto com unidades adequadas===========

# fzr merge de cod_produto e sig_unidmed
df_producao_bruto = pd.merge(
        left=df_ibama_EF, #tabela unida a esqueda
        right=df_unidades, #tabela unida a direita
        left_on=['cod_produto', 'sig_unidmed'], #chaves da tabela a esqueda
        right_on=['cod_produto', 'sig_unidmed'], #chaves da tabela a direita
        how='left', # todas as linhas da tabela da esquerda (df_ibama_EF) serão mantidas.
    )

#multiplicar a produção pelo fatorConversão
#inventário bruto sem ajuste de outliers
df_producao_bruto['Produção (Ton ou hL)'] = (df_producao_bruto['qtd_produzida'].astype(float) * 
                                       df_producao_bruto['fatorConversao'].astype(float))

# Remover linhas onde producao é zero (unidades incoerentes)
# limpar colunas não necessárias e/ou repetidas
colunas_remover = ['Unit_y','nom_produto_y','unidade_medida_y','CODIGO DA CATEGORIA',
                   'CODIGO DA ATIVIDADE','ANO_INICIO','ANO_FIM','qtd_instalada',
                   'informacao_sigilosa','tipo_sigilo','info_sigilo','_merge']

df_producao_notnull = df_producao_bruto[df_producao_bruto['Produção (Ton ou hL)'] != 0].copy().drop(columns=colunas_remover)


#%%=============== Remoção de outliers e calculo das emissões ==================

#Ajuste dos outliers de produção
df_producao = tratamento_outliers(df_producao_notnull)

#Remover algumas colunas
colunas_remover = ['CNPJ', 'MUNICIPIO', 'SITUACAO CADASTRAL', 'mv.nom_pessoa', 'unidade_medida_x',
                   'sig_unidmed', 'nom_produto_x', 'qtd_produzida', 'lei_sigilo','mv.sig_uf', 'tipo',
                   'Reference', 'contagem','Fonte', 'Observaçoes', 'obs_tratamento']

df_producao.columns
#Cálculo das emissões
df_inventario = df_producao.copy().drop(columns=colunas_remover)
df_inventario['Emissão NMCOV (kg)'] = (df_inventario['Produção (Ton ou hL)'] * df_inventario['Value'].astype(float))
df_inventario['Emissão NMCOV CI_lower (kg)'] = (df_inventario['Produção (Ton ou hL)'] * df_inventario['CI_lower'].astype(float))
df_inventario['Emissão NMCOV CI_upper (kg)'] = (df_inventario['Produção (Ton ou hL)'] * df_inventario['CI_upper'].astype(float))


#Exportar para realizar análises em outro código
df_inventario.to_csv(os.path.join(repo_path,'outputs','inventarioEmissoesIndustriaisIndustriaAlimenticiaBR.csv'), index = False)