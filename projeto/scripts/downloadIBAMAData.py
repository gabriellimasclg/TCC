# -*- coding: utf-8 -*-
"""
Created on Sun Apr 13 11:32:03 2025

@author: Gabriel
"""

import os
import requests
import urllib3
import pandas as pd

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
#%%  Função para download do CSV ibama de PJ por estado

def download_ibama_ctf_data(uf,base_dir):
        
    # URL base (sem a UF)
    base_url = "http://dadosabertos.ibama.gov.br/dados/CTF/APP/"
        
    # Cria a pasta para armazenar os dados (se não existir)
    output_dir = f"{base_dir}dadosIbama"
    os.makedirs(output_dir, exist_ok=True)
    

    try:
        # Monta a URL completa
        url = f"{base_url}{uf}/pessoasJuridicas.csv"
            
        print(f"Baixando dados de {uf}...")
            
        # Faz o download
        response = requests.get(url, verify=False)
        response.raise_for_status()  # Verifica se houve erro
            
        # Define o caminho do arquivo de saída
        output_path = os.path.join(output_dir, f"pessoasJuridicas_{uf}.csv")
            
        # Salva o arquivo
        with open(output_path, 'wb') as f:
            f.write(response.content)
                
        print(f"Dados de {uf} salvos em {output_path}")
            
    except requests.exceptions.RequestException as e:
        print(f"Erro ao baixar dados de {uf}: {e}")

# Lista de todas as UFs do Brasil
dataDir = 'E:/_code/TCC/projeto/outputs/'

ufs = [
    'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO',
    'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI',
    'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
]

for uf in ufs:
    download_ibama_ctf_data(uf,dataDir)
    
#%% Função que abre e concatena CSVs

#Ele pode ser "juntado" com a função acima, mas eu não quis rodar de novo o 
#download, que já demorou o suficiente kk


# Lista para armazenar os DataFrames
pessoasJuridicas_BR = []

# Listar arquivos CSV
dataList = [f for f in os.listdir(os.path.join(dataDir, 'dadosIbama')) 
           if f.endswith('.csv') and f.startswith('pessoasJuridicas')]

for fileInList in dataList:
    file_path = os.path.join(dataDir, 'dadosIbama', fileInList)
    
    try:      
        dfConc = pd.read_csv(file_path,delimiter=';',dtype={'CNPJ': str},keep_default_na=False)
        dfConc['CNPJ'] = dfConc['CNPJ'].str.zfill(14)  # Preenche com zeros à esquerda até ter 14 dígitos
        dfConc.columns = dfConc.columns.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
        pessoasJuridicas_BR.append(dfConc)
        
    except Exception as e:
        print(f"Erro no arquivo {fileInList}: {e}")


# Concatenar todos os DataFrames da lista em um único DataFrame
df_final = pd.concat(pessoasJuridicas_BR, ignore_index=True)  # ignore_index evita duplicação de índices

# Exportar para CSV (correção: caminho completo e extensão .csv)
df_final.to_csv(os.path.join(f'{dataDir}dadosIbama', "PJ_BR.csv"), 
                index=False, encoding = 'utf-8')  # index=False evita salvar os índices    
    
print(df_final.columns)    
    
    
    
    
    
    
    
    
    