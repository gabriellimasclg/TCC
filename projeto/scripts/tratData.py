# -*- coding: utf-8 -*-
"""
Created on Sun Apr 13 13:15:24 2025

@author: Gabriel
"""

import os
import requests
import urllib3
import pandas as pd
import numpy as np
import pandas as pd
import unicodedata



#%% Leitura das bases de dados

df_cnpj = pd.read_csv(r'C:\Users\Gabriel\OneDrive\GitHub\TCC\projeto\outputs\dadosIbama\PJ_BR.csv',dtype={'CNPJ': str})

# Lê as duas abas do Excel
aba1 = pd.read_excel(r'C:\Users\Gabriel\OneDrive\GitHub\TCC\projeto\inputs\IBAMA\RAPP.xlsx',
                     sheet_name=0, dtype={'mv.num_cpf_cnpj': str})  # Primeira aba
aba2 = pd.read_excel(r'C:\Users\Gabriel\OneDrive\GitHub\TCC\projeto\inputs\IBAMA\RAPP.xlsx',
                     sheet_name=1, dtype={'mv.num_cpf_cnpj': str})  # Segunda aba

# Concatena as duas abas verticalmente
df_ibama = pd.concat([aba1, aba2], ignore_index=True)

#%% Criando cópias

df_ibama_clean = df_ibama.copy()
df_cnpj_clean = df_cnpj.copy()

df_ibama_clean = df_ibama_clean.drop_duplicates()

#%% Função para padronizar texto
def clean_text(text):
    if pd.isna(text):
        return text
    # Converte para string, remove espaços extras, acentos e coloca em maiúsculas
    text = str(text).strip().upper()
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
    return text

# Padroniza municípios (remove acentos, espaços e coloca em maiúsculas)
df_cnpj_clean['Municipio'] = df_cnpj['Municipio'].apply(clean_text)
df_ibama_clean['mv.nom_municipio'] = df_ibama['mv.nom_municipio'].apply(clean_text)
df_cnpj_clean['Razao Social'] = df_cnpj_clean['Razao Social'].apply(clean_text)
df_ibama_clean['mv.nom_pessoa'] = df_ibama_clean['mv.nom_pessoa'].apply(clean_text)

#filtrar cnpj ativo
df_cnpj_clean = df_cnpj_clean[df_cnpj_clean['Situacao cadastral']=='Ativa']

#%% merge dos dataframes
# essas são as que me interessam, então vou tirar duplicatas referentes a outros campos
'''
Dúvida:
    - Uma mesma indústria com o mesmo cnpj e lat lon produz produtos diferentes
    - ou seja, multiplos codigo de categoria e codigo de atividade
    - ao fazer o merge, ele encontra aquele cnpj e municipio e repete a linha
    da base de dados do ibama, pois tem diferentes correspondencias na base de
    dados de CNPJ.
    - Problema: Quantidade de produção repete, fica incoerente. O que fazer?
    SOLUÇÂO MOMENTÂNEA: Vou ignorar o código de categoria e atividade em prol
    fazer o MERGE e obter as coordenadas.
'''

# 
cols_agrupamento = ['CNPJ', 'Municipio', 'Latitude', 'Longitude','Codigo da categoria','Codigo da atividade'] 
df_cnpj_clean = df_cnpj_clean.drop_duplicates(subset=cols_agrupamento)

#merge das bases de dados, para obter coordenada e código da atividade
df_ibama_completo = pd.merge(
    left=df_cnpj_clean,
    right=df_ibama_clean, 
    left_on = ['CNPJ','Municipio'],
    right_on = ['mv.num_cpf_cnpj','mv.nom_municipio'],
    how='right',  
    indicator=True
)

# Verificar quantas linhas têm correspondência
#Alguns não estão no cadastro, aparentemente, ou eu q fiz algo errado
print(df_ibama_completo['_merge'].value_counts())

teste = df_ibama_completo['nom_produto'].unique()
df_cnpj_clean.columns
df_ibama_completo.columns

#%% vou fzr a "tarefa" do vinho aqui
# ENCONTREI O MSM PROBLEMA Q DESCREVI ACIMA

df_cnpj_vinho = df_cnpj_clean[
    (df_cnpj_clean['Codigo da categoria'] == 16) & 
    (df_cnpj_clean['Codigo da atividade'] == 11)
]

df_ibama_vinho = pd.merge(
    left=df_cnpj_vinho,
    right=df_ibama_clean, 
    left_on = ['CNPJ','Municipio'],
    right_on = ['mv.num_cpf_cnpj','mv.nom_municipio'],
    how='left',  
    indicator=True
)




















#%% Verificando CNPJs
# Versão correta (com .str.len() para verificar cada valor)
df_ibama['tipo'] = np.where(df_ibama['mv.num_cpf_cnpj'].str.len() == 14, 'CNPJ',
                            np.where(df_ibama['mv.num_cpf_cnpj'].str.len() == 11,'CPF',
                                     'outro'))



contagem = {4:0,5:0,6:0,7:0,8:0,9:0,10:0,11:0,12: 0, 13: 0, 14: 0}
total_cnpjs = len(df_ibama)

for cnpj in df_ibama['mv.num_cpf_cnpj']:
    cnpj_limpo = ''.join(filter(str.isdigit, str(cnpj)))
    tamanho = len(cnpj_limpo)
    
    if tamanho in contagem:
        contagem[tamanho] += 1

# Soma todas as contagens específicas
total_contado = sum(contagem.values())

# Verificação
if total_contado == total_cnpjs:
    print("✅ Todos os CNPJs foram contabilizados!")
else:
    print(f"⚠️ Atenção: {total_cnpjs - total_contado} CNPJs não se enquadram nos tamanhos 12-14 dígitos")

# Imprime os resultados detalhados
for tamanho, quantidade in contagem.items():
    print(f'{quantidade:>5} CNPJs com {tamanho:>2} dígitos ({quantidade/total_cnpjs:.1%})')

print(f"\nTotal contado: {total_contado} de {total_cnpjs} CNPJs")

