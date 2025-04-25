# -*- coding: utf-8 -*-
"""
Created on Wed Apr 23 07:47:09 2025

@author: Gabriel
"""

import os
import numpy as np
from download_database import download_ibama_ctf_data, download_ef_eea_tier2
from import_database import ibama_production_data, import_products_code
from merge_filter_df import merge_cnpj_prod
from merge_filter_df import filter_activity_category

if __name__ == "__main__":
    try:
        repo_path = os.path.dirname(os.getcwd())
        df_ibama_cnpj = download_ibama_ctf_data(repo_path)
        print("Dados baixados e processados com sucesso!")
        df_ibama_prod = ibama_production_data(repo_path)
        df_ibama = merge_cnpj_prod(df_ibama_cnpj,df_ibama_prod)
    except Exception as e:
        print(f"Falha: {e}")

# arrumar essa seleção do vinho, tá dando erro
pares_vinho = ["16-11"]  # Somente vinho
df_vinho = filter_activity_category(df_ibama, pares_vinho)

eea_ef = pd.read_csv(os.path.join(repo_path, 'inputs', 'ef_eea_tier2.csv'))

#%% ABAIXO A PARTE DO CÓDIGO QUE NÃO AJUSTEI PARA A BELEZA E PERFEIÇÃO
'''
Ideias: Fazer um dicionário com: 
    - código do produto vs código de categoria-atividade
    - código de categoria-atividade vs código EEA
'''
#%%



'''
DÚVIDA: E as diversas variações de 1112. (ex: vermute, sidra, que é feito de vinho)
Mostrar link dos produtos para tirar dúvida
'''
# vi nos códigos do ibama que vinho tem cod_produto que começa com 1112.
# vinhos terminam com 2060;2070;2080.
# Verificar online: https://servicos.ibama.gov.br/ctfcd/manual/html/lista_produtos.htm
# Baixar excel: https://www.ibge.gov.br/estatisticas/metodos-e-classificacoes/classificacoes-e-listas-estatisticas/9153-lista-de-produtos-da-industria.html

cod_produto= import_products_code(repo_path)
#arrumar p tirar colunar q começam com CNAE

codigo_interesse = {'Vinho' : {'cod_produto':['1112.2060','1112.2070','1112.2080'],
                               'NFR':,'2.H.2'
                               'SNAP':['040606']}
                    }

filtro_codigo = (
    df_vinho['cod_produto'].astype(str).str.startswith('1112') & 
    df_vinho['cod_produto'].astype(str).str.endswith(('', '', ''))
)

df_vinho_filtrado = df_vinho[filtro_codigo]

#%% Conversão de unidades para hl (hectolitros, 1hL = 100 L); densidade do vinho = 1 g/L OU kg/m³
# uma caixa de vinho tem 6 garrafas
# uma garrafa de vinho tem 750 mL
# uma lata de vinho tem 250 mL
vinho_barra = df_vinho_filtrado[df_vinho_filtrado['unidade_medida']=='Barra (BA)']

df_vinho_filtrado['unidade_medida'].value_counts()

fator_conversao_para_hl = {
    # Unidades baseadas em litros:
    'Litro (L)': 0.01,            # 1 L = 0.01 hL (pois 100 L = 1 hL)
    'Mililitro (ML)': 0.00001,     # 1.000 mL = 1 L → 100.000 mL = 1 hL → 1 mL = 0.00001 hL
    'Metro Cúbico (M3)': 10,       # 1 m³ = 1.000 L = 10 hL
    'Decilitro (DL)': 0.001,       # 1 dL = 0.1 L → 0.001 hL

    # Unidades de massa (assumindo densidade do vinho ≈ 1 kg/L):
    'kilogramas (kg)': 0.01,       # 1 kg ≈ 1 L → 0.01 hL
    'Tonelada (TON)': 10,          # 1 ton = 1.000 kg ≈ 1.000 L = 10 hL
    'Gramas (g)': 0.00001,         # 1.000 g = 1 kg ≈ 1 L → 0.01 hL

    # Unidades comerciais de vinho:
    'Unidade (UN)': 0.0075,        # Garrafa de 750 mL = 0.75 L → 0.0075 hL
    'Caixa (CX)': 0.045,           # Caixa com 6 garrafas de 750 mL = 4,5 L → 0.045 hL
    'Barra (BA)': 2.25,          # Barra deve ser barril de vinho ≈ 117 L (varia por região)

    # Padrão para unidades não mapeadas:
    'default': np.nan
}

df_vinho_hl = df_vinho_filtrado.copy()
df_vinho_hl['volume_hl'] = df_vinho_filtrado.apply(
    lambda row: row['qtd_produzida'] * fator_conversao_para_hl.get(
        row['unidade_medida'], 
        fator_conversao_para_hl['default']
    ),
    axis=1
)


#%% cálculo do fator de emissão
# no documento da EEA, tabelas 3.26 até 3.28 para vinhos
'''
Dúvida: Adotei o 0.08 pq as bases de dados não batem mt. Escrevi sobre no onenote
'''
df_vinho_ef = df_vinho_hl.copy()
df_vinho_ef['NMVOC EF (kg/hl of wine)'] = 0.08
df_vinho_ef['NMVOC (kg)'] = df_vinho_ef['NMVOC EF (kg/hl of wine)']*df_vinho_ef['volume_hl']


#%% plotar
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