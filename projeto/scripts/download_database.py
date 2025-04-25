# -*- coding: utf-8 -*-
"""
Created on Sun Apr 13 11:32:03 2025

@author: Gabriel
"""

import urllib3
import requests
import os
import pandas as pd
from clean_text import clean_text
from fake_useragent import UserAgent

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def download_ibama_ctf_data(repo_path):
    '''
    Baixa dados de CNPJs por UF do IBAMA e os consolida.

    Parâmetros:
        repo_path (str): Caminho raiz do repositório (as pastas inputs/ e outputs/ serão criadas aqui).

    Retorna:
        DataFrame com os dados consolidados ou None se falhar.
    '''
    if not os.path.exists(repo_path):
        raise ValueError(f"Caminho não encontrado: {repo_path}")
    
    raw_dir = os.path.join(repo_path, 'inputs', 'dadosIbamaCNPJ', 'raw')  # Para arquivos brutos por UF
    processed_dir = os.path.join(repo_path, 'outputs', 'dadosIbamaCNPJ', 'processed')  # Para o arquivo consolidado
    
    # Garante que as pastas existam
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    
    base_url = "http://dadosabertos.ibama.gov.br/dados/CTF/APP/"
    
    ufs = [
        'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO',
        'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI',
        'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
    ]
    
    pessoas_juridicas_br = []
    
    for uf in ufs:
        try:
            url = f"{base_url}{uf}/pessoasJuridicas.csv"
            print(f"Baixando {uf}...")
            
            response = requests.get(url, verify=False, timeout=30)  # ATENÇÃO: verify=False é inseguro!
            response.raise_for_status()
            
            # Salva o arquivo bruto
            output_path = os.path.join(raw_dir, f"pessoasJuridicas_{uf}.csv")
            with open(output_path, 'wb') as f:
                f.write(response.content)
            
            # Processa o arquivo
            try:
                df = pd.read_csv(output_path, delimiter=';', dtype={'CNPJ': str, 'Codigo da atividade': str, 'Codigo da categoria': str}, keep_default_na=False)
                df['CNPJ'] = df['CNPJ'].str.zfill(14)  # Padroniza CNPJ
                df.columns = df.columns.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
                pessoas_juridicas_br.append(df)
            except Exception as e:
                print(f"Erro ao processar {uf}: {e}")
                
        except requests.exceptions.RequestException as e:
            print(f"Erro ao baixar {uf}: {e}")
    
    # Consolida e salva
    if pessoas_juridicas_br:
        df_final = pd.concat(pessoas_juridicas_br, ignore_index=True)
        df_final['Municipio'] = df_final['Municipio'].apply(clean_text)
        df_final['Razao Social'] = df_final['Razao Social'].apply(clean_text)

        output_file = os.path.join(processed_dir, "pessoasJuridicas_BR.csv")
        df_final.to_csv(output_file, index=False, encoding='utf-8')
        return df_final
    return None
    

        
        

    
    
    
    