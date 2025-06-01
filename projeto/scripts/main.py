# -*- coding: utf-8 -*-
"""
Created on Wed Apr 23 07:47:09 2025

@author: Gabriel
"""
#%%=========================== Bibliotecas ==================================
import os
import numpy as np
import pandas as pd
from download_database import download_ibama_ctf_data
from import_database import ibama_production_data, import_products_code
from merge_filter_df import merge_cnpj_prod, conecta_ibama_ef, converter_para_hl

#%%===============================TCC========================================

# Caminho da pasta do projeto
repo_path = os.path.dirname(os.getcwd())

# Pega dados de CNPJ + coordenadas
df_ibama_cnpj = download_ibama_ctf_data(repo_path) #dados de cnpj
df_ibama_cnpj['Municipio'] = df_ibama_cnpj['Municipio'].str.replace(
    r"TRAJANO DE MORAIS", "TRAJANO DE MORAES", regex=True
    )

# DF com Dados de produção com CNPJ + Código de Produto + Produção
df_ibama_prod = ibama_production_data(repo_path) #dados de produção
df_ibama_prod['mv.nom_municipio'] = df_ibama_prod['mv.nom_municipio'].str.strip()
df_ibama_prod['mv.nom_municipio'] = df_ibama_prod['mv.nom_municipio'].str.replace(
    r"SANT'? ?ANA DO LIVRAMENTO", "SANTANA DO LIVRAMENTO", regex=True
    )
df_ibama_prod['mv.nom_municipio'] = df_ibama_prod['mv.nom_municipio'].str.replace(
    r"PRESIDENTE CASTELLO BRANCO", "PRESIDENTE CASTELO BRANCO", regex=True
    )

# Falar que tem algumas cidades que ainda não conectaram e ver o que fazer quanto
# a cpf e cnpj --> ideia é pegar coordenada do municipio geral

'''
# Municípios únicos em cada base
municipios_prod = set(df_ibama_prod['mv.nom_municipio'].unique())
municipios_cnpj = set(df_ibama_cnpj['Municipio'].unique())

# Presentes em ambas
presentes_ambas = municipios_prod & municipios_cnpj

# Presentes apenas na primeira
apenas_prod = municipios_prod - municipios_cnpj

# Presentes apenas na segunda
apenas_cnpj = municipios_cnpj - municipios_prod
'''
df_dd = df_ibama_prod['mv.nom_municipio'].value_counts()

# DF com Produção + Código de produto + Coordenadas
df_ibama = merge_cnpj_prod(df_ibama_cnpj,df_ibama_prod) #mesclar p obter coordenadas e código de atividade

#Base de dados com todos os Códigos de Produto
cod_produto= import_products_code(repo_path)

# Vou exportar para classificar MANUALMENTE Código de Produto (IBAMA) vs NFR+Table (EEA)
cod_produto.to_excel(os.path.join(repo_path, 'outputs', 'cod_produto_tratado.xlsx'))

# RENOMEEI 'cod_produto_tratado.xlsx' para 'cod_produto_nfr_table.xlsx' para
# não correr o risco de perder o material por sobreposição ao rodar o código

#Importar DF no qual eu conecto MANUALMENTE o código do produto com NFR e TABLE
cod_produto_nfr_table = pd.read_excel(os.path.join(repo_path, 'inputs', 'cod_produto_nfr_table.xlsx'),
                                      dtype={'PRODLIST': str})

# DF com Fatores de emissão + NFR + Table
eea_ef = pd.read_csv(os.path.join(repo_path, 'inputs', 'ef_eea_tier2.csv'))
eea_ef['Table'] = eea_ef['Table'].str.replace('Table_', '', regex=False)

# DF com Fator de emissão + NFR + Table + Código do Produto + Produção + Coordenadas
df = conecta_ibama_ef(df_ibama,eea_ef,cod_produto_nfr_table)

'''
A conversão de unidades de medida precisará ser automatizda.
tipo, se for unidade X --> dicionario X de fator de conversão
'''

#%%===========================TRABALHO DO LEO==================================

# A partir daqui, vou filtrar apenas as bebidas, que eu classifiquei, para o 
#trabalho da disciplina ENS5132

#Filtrando as bebidas no df conector, que vou usar no trabalho do Leo
df_bebidas = df[
    df['cod_produto'].astype(str).str.startswith(('1113','1112','1111'))
]

# Trabalhando com fatores de conversão de unidades
df_bebidas['unidade_medida'].value_counts()

#Aqui eu estudo as unidades pois estou montando um csv de fator de conversão
df_cerveja = df_bebidas[df_bebidas['cod_produto'].astype(str).str.startswith('1113')]
df_cerveja['unidade_medida'].value_counts()

df_destilado = df_bebidas[df_bebidas['cod_produto'].astype(str).str.startswith('1111')]
df_destilado['unidade_medida'].value_counts()

# Carrega o CSV de conversão de unidades
df_conversao = pd.read_excel(os.path.join(repo_path, 'inputs', 'conversao_unidades.xlsx'))


# Aplica a função de conversão de unidades
df_bebidas['volume_hl'] = df_bebidas.apply(
    lambda row: converter_para_hl(
        df_conversao,
        row['qtd_produzida'],
        row['unidade_medida'],
        row.get('cod_produto')  # Usa .get() para caso a coluna não exista
    ),
    axis=1
)

# Faz o cálculo de NMVOC (kg)
df_bebidas['Value'] = pd.to_numeric(df_bebidas['Value'], errors='coerce')
df_bebidas['volume_hl'] = pd.to_numeric(df_bebidas['volume_hl'], errors='coerce')

mask = df_bebidas['volume_hl'].notna()
df_bebidas.loc[mask, 'NMVOC (kg)'] = df_bebidas.loc[mask, 'Value'] * df_bebidas.loc[mask, 'volume_hl']

# Calcula a porcentagem de valores nan (pra tentar assumir mais unidades)
total_valores = len(df_bebidas['NMVOC (kg)'])
total_nan = df_bebidas['NMVOC (kg)'].isna().sum()

print(f"Total de valores: {total_valores}")
print(f"Valores NaN: {total_nan}")
print(f"Porcentagem de NaN: {(total_nan/total_valores)*100:.2f}%")

#base para bruno analisar
df_bebidas_enviar = df_bebidas.drop(columns=['CNPJ','mv.num_cpf_cnpj','mv.nom_pessoa'])
df_bebidas_enviar.to_excel(os.path.join(repo_path, 'outputs', 'df_bebidas_para_analise.xlsx'))
#%% PARTE DO BRUNO














#%% plotar

'''
Fazer função de plotagem, estudar possibildiades de plotagem.
Perguntar ideias de análise estatística p trabalho do Leo :((
'''

import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import contextily as ctx
import numpy as np

# 1. Load Brazil map from Natural Earth
try:
    # Try loading from local cache first
    brasil = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    brasil = brasil[brasil['name'] == 'Brazil'].to_crs("EPSG:4326")
except:
    # Fallback to direct download
    naturalearth_url = "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip"
    brasil = gpd.read_file(naturalearth_url)
    brasil = brasil[brasil['SOVEREIGNT'] == 'Brazil'].to_crs("EPSG:4326")

# 2. Prepare your data
gdf = gpd.GeoDataFrame(
    df_vinho_ef,
    geometry=gpd.points_from_xy(
        df_vinho_ef['Longitude'].str.replace(',', '.').astype(float),
        df_vinho_ef['Latitude'].str.replace(',', '.').astype(float)
    ),
    crs="EPSG:4326"
)

# 3. Create figure
fig, ax = plt.subplots(figsize=(14, 12))

# 4. Plot Brazil map
brasil.plot(ax=ax, color='lightgray', edgecolor='dimgray', alpha=0.5)

# 5. Create hexbin heatmap
hexplot = ax.hexbin(
    x=gdf.geometry.x,
    y=gdf.geometry.y,
    C=gdf['NMVOC (kg)'],
    gridsize=25,  # Fewer grids for better performance
    cmap='Reds',
    reduce_C_function=np.sum,
    alpha=0.85,
    edgecolor='none',
    norm=colors.LogNorm(),  # Log scale for better visualization
    mincnt=1  # Only show hexagons with data
)

# 6. Add basemap
ctx.add_basemap(
    ax,
    crs=gdf.crs.to_string(),
    source=ctx.providers.CartoDB.Positron,
    zoom=5,
    alpha=0.8  # Slightly transparent
)

# 7. Add colorbar
cbar = plt.colorbar(hexplot, ax=ax, shrink=0.5)
cbar.set_label('Total NMVOC Emissions (kg) - Log Scale', fontsize=11)

# 8. Final touches
plt.title("NMVOC Emissions from Wine Industry in Brazil", fontsize=16, pad=20)
plt.xlabel("Longitude", fontsize=12)
plt.ylabel("Latitude", fontsize=12)
plt.grid(visible=True, linestyle='--', alpha=0.3)

plt.tight_layout()

output = r'E:\_code\TCC\projeto\outputs'
output_file = "emissões_nmvoc_brasil.png"  # ou .pdf, .svg, .jpg
plt.savefig(
    output + '/' + output_file,
    dpi=300,               # Resolução (300 para boa qualidade de impressão)
    bbox_inches='tight',   # Remove bordas brancas em excesso
    facecolor='white',     # Fundo branco
    format='png'           # Formato do arquivo
)

plt.show()