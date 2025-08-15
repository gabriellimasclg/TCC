# -*- coding: utf-8 -*-
"""
Created on Tue Aug 12 10:02:47 2025

@author: glima
"""

#%% Importando bibliotecas necessárias

import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np
import pymannkendall as mk
from analiseDados import analisar_tendencia_nmvc, plot_emissao
import geopandas as gpd
from clean_text import clean_text
from shapely.geometry import box
import xarray as xr
import unicodedata
from matplotlib.colors import LogNorm

#%% Definindo Paths e importando 
repo_path = os.path.dirname(os.getcwd())
figpath = os.path.join(repo_path,'figures')

#importar csv com inventário
df = pd.read_csv(os.path.join(repo_path,'inputs','inventarioEmissoesIndustriaisIndustriaAlimenticiaBR.csv'))
df['LONGITUDE'] = df['LONGITUDE'].str.replace(',', '.', regex=False).astype(float)
df['LATITUDE'] = df['LATITUDE'].str.replace(',', '.', regex=False).astype(float)

#%% análises e gráficos iniciais

# Análise de tendência no BR
tendênciaBR = analisar_tendencia_nmvc(df, ['NFR'])

# Análise de tendência por estado
tendênciaUF = analisar_tendencia_nmvc(df, ['ESTADO'])

plot_emissao(df,figpath)

plot_emissao(df, figpath,coluna='ESTADO')


#%% Geração do cube-data

# Carregar os estados do Brasil via GeoJSON
url = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
estados = gpd.read_file(url)
estados['Estado'] = estados['name'].apply(clean_text) # Agora 'clean_text' existe

# Função para criar a grade
def CreateGrid(Tam_pixel, minx, maxx, miny, maxy):
    x_coords = np.arange(minx, maxx, Tam_pixel)
    y_coords = np.arange(miny, maxy, Tam_pixel)
    grid_cells = [box(x, y, x + Tam_pixel, y + Tam_pixel) for x in x_coords for y in y_coords]
    gridGerado = gpd.GeoDataFrame(geometry=grid_cells, crs='EPSG:4674')
    
    # Extraindo coordenadas do centroide
    gridGerado['lon'] = gridGerado.geometry.centroid.x
    gridGerado['lat'] = gridGerado.geometry.centroid.y
    
    # Determinando as coordenadas de cada célula 
    xx, yy = np.meshgrid(np.sort(gridGerado['lon'].unique()),
                         np.sort(gridGerado['lat'].unique()))
    
    return gridGerado, xx, yy

# Definição da área e resolução
xmin, xmax = -74, -34  # longitude
ymin, ymax = -34, 6    # latitude
res = 0.5              # resolução do pixel em graus adotada

grid_base, xx, yy = CreateGrid(res, xmin, xmax, ymin, ymax)

# Filtrar dados válidos
df_geo = df.dropna(subset=['LATITUDE', 'LONGITUDE', 'Emissão NMCOV (kg)'])

# Criar GeoDataFrame
gdf_emissoes = gpd.GeoDataFrame(
    df_geo,
    geometry=gpd.points_from_xy(
        df_geo['LONGITUDE'].astype(float),
        df_geo['LATITUDE'].astype(float)
    ),
    crs='EPSG:4674'
)

# Junção espacial com a grade
pontos_na_grade = gpd.sjoin(gdf_emissoes, grid_base, how="inner", predicate="within")

# Agrupar por célula, ano e estado
emissoes_agregadas = (
    pontos_na_grade
    .groupby(['index_right', 'num_ano', 'ESTADO'], as_index=False)['Emissão NMCOV (kg)']
    .sum()
    .rename(columns={'index_right': 'grid_id'})
)

# --- MELHORIA DE PERFORMANCE: PREENCHIMENTO OTIMIZADO DO CUBO ---

# 1. Juntar os dados agregados com as coordenadas da grade
#    Isso evita ter que buscar a 'lat' e 'lon' dentro de um loop
emissoes_com_coords = pd.merge(
    emissoes_agregadas,
    grid_base[['lat', 'lon']],
    left_on='grid_id',
    right_index=True
)

# 2. Definir o índice com as futuras dimensões do cubo Xarray
emissoes_com_indice = emissoes_com_coords.set_index(['ESTADO', 'num_ano', 'lat', 'lon'])

# 3. Converter diretamente de um objeto Pandas (com MultiIndex) para Xarray
#    Esta operação é vetorizada e ordens de magnitude mais rápida que o loop.
data_array = xr.DataArray.from_series(emissoes_com_indice['Emissão NMCOV (kg)'])

# Renomear as dimensões para o padrão desejado
data_array = data_array.rename({
    'ESTADO': 'estado',
    'num_ano': 'time'
})

# Opcional: Reordenar as dimensões se desejar uma ordem específica
data_array = data_array.transpose('estado', 'time', 'lat', 'lon')

# Criar o Dataset final
ds_emissoes_completo = xr.Dataset({'nmvoc_emissions': data_array})

# Preencher valores ausentes (NaN) com 0, se fizer sentido para a sua análise
ds_emissoes_completo = ds_emissoes_completo.fillna(0)

print("Cubo de dados criado com sucesso!")
print(ds_emissoes_completo)






#%% Geração do gráfico de emissão agregada pixelada nacional

#agregação dos dados
emissoes_total_brasil = ds_emissoes_completo['nmvoc_emissions'].sum(dim='estado')

# Calcular o valor máximo de emissão em qualquer ano
vmax_global = emissoes_total_brasil.max().item()

# Para a escala log, o mínimo deve ser > 0. 
# Encontramos o menor valor de emissão positivo em qualquer ano para usar como vmin.
vmin_global = emissoes_total_brasil.where(emissoes_total_brasil > 0).min().item()
norm_obj = LogNorm(vmin=vmin_global, vmax=vmax_global)
cbar_label = 'Emissão Agregada de NMVOC (kg) [Escala Log]'

# Define os anos que serão plotados
anos = emissoes_total_brasil['time'].values

# Cria a figura e a grade de subplots. 
fig, axes = plt.subplots(
    nrows=3, 
    ncols=3, 
    figsize=(12, 15), # <-- AJUSTE: Figura quadrada para um grid 3x3
    constrained_layout=True
)

# Transforma a matriz 2D de eixos (axes) em uma lista 1D para facilitar o loop
axes_flat = axes.flatten()

# CONFIGURAÇÕES DE CADA SUBPLOT
# Itera sobre cada ano e seu respectivo eixo (ax) no grid
for i, ano in enumerate(anos):
    
    # Seleciona o eixo atual para plotar
    ax = axes_flat[i]
    
    # Seleciona a 'fatia' 2D do cubo de dados para o ano atual
    data_slice_ano = emissoes_total_brasil.sel(time=ano)
          
    # Camada de fundo com os limites dos estados
    estados.boundary.plot(ax=ax, linewidth=0.6, color='gray', zorder=2)
    
    # Camada de dados (heatmap)
    mappable = data_slice_ano.plot(
        ax=ax,
        cmap='YlOrRd',
        add_colorbar=False,
        norm=norm_obj,
        zorder=1
    )
    
    # Customização de cada subplot
    ax.set_title(str(ano), fontsize=14)
    ax.set_aspect('equal')
    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)
    ax.set_xlabel('')
    ax.set_ylabel('')
    ax.tick_params(axis='both', which='both', bottom=False, top=False, left=False, right=False, labelbottom=False, labelleft=False)
    for spine in ax.spines.values():
        spine.set_visible(False)

# Esconde os eixos que não foram usandos
for i in range(len(anos), len(axes_flat)):
    axes_flat[i].set_visible(False)
    
# CONFIGURAÇÕES FINAIS DA FIGURA COMPLETA

# Adiciona um título principal para todo o painel
fig.suptitle('Evolução Anual da Emissão Agregada de NMVOC no Brasil (kg)', fontsize=22, weight='bold') # Título ajustado

# Adiciona a barra de cores compartilhada na parte inferior
cbar = fig.colorbar(
    mappable, 
    ax=axes.ravel().tolist(),
    orientation='horizontal',
    location='bottom',
    shrink=0.6,
    pad=0.04, 
    aspect=40 # Barra um pouco mais longa
)

plt.savefig(os.path.join(figpath,'Emissões NMCOV no Brasil Anual Geolocalizada.png'), dpi=300, bbox_inches='tight')

#%% ele gerou algo, mas a partir daqui ta tudo meio estranho e preciso estudar

# -*- coding: utf-8 -*-
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd
import xarray as xr
import pymannkendall as mk
from shapely.geometry import box
from matplotlib.colors import LogNorm
from matplotlib.patches import Patch
from matplotlib.colors import ListedColormap

# -------------------------------------------------------------------------
# PATHS
repo_path = os.path.dirname(os.getcwd())
figures_path = os.path.join(repo_path, 'figures_paineis_estados')
os.makedirs(figures_path, exist_ok=True)

# -------------------------------------------------------------------------
# CARREGAMENTO DOS DADOS
df = pd.read_csv(os.path.join(repo_path,'inputs','inventarioEmissoesIndustriaisIndustriaAlimenticiaBR.csv'))
df['LONGITUDE'] = df['LONGITUDE'].str.replace(',', '.').astype(float)
df['LATITUDE']  = df['LATITUDE'].str.replace(',', '.').astype(float)

# -------------------------------------------------------------------------


# -------------------------------------------------------------------------
# FUNÇÕES AUXILIARES
def CreateGrid(Tam_pixel, minx, maxx, miny, maxy):
    x_coords = np.arange(minx, maxx, Tam_pixel)
    y_coords = np.arange(miny, maxy, Tam_pixel)
    grid_cells = [box(x, y, x+Tam_pixel, y+Tam_pixel) for x in x_coords for y in y_coords]
    gdf = gpd.GeoDataFrame(geometry=grid_cells, crs='EPSG:4674')
    gdf['lon'] = gdf.geometry.centroid.x
    gdf['lat'] = gdf.geometry.centroid.y
    return gdf

ALPHA = 0.05
def calcular_tendencia_pixel(serie_temporal_pixel):
    if len(serie_temporal_pixel) < 3: return np.array(np.nan)
    try:
        resultado = mk.original_test(serie_temporal_pixel, alpha=ALPHA)
        if resultado.p < ALPHA:
            if resultado.trend == 'increasing': return np.array(1)
            elif resultado.trend == 'decreasing': return np.array(-1)
        return np.array(0)
    except:
        return np.array(np.nan)

# -------------------------------------------------------------------------
# CUBO DE DADOS
url = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
estados = gpd.read_file(url)
estados['Estado'] = estados['name'].str.upper()

xmin, xmax, ymin, ymax, res = -74, -34, -34, 6, 1.0
grid_base = CreateGrid(res, xmin, xmax, ymin, ymax)

df_geo = df.dropna(subset=['LATITUDE','LONGITUDE','Emissão NMCOV (kg)'])
gdf_emissoes = gpd.GeoDataFrame(df_geo, geometry=gpd.points_from_xy(df_geo['LONGITUDE'], df_geo['LATITUDE']), crs='EPSG:4674')
pontos_na_grade = gpd.sjoin(gdf_emissoes, grid_base, how="inner", predicate="within")

emissoes_agregadas = pontos_na_grade.groupby(['index_right','num_ano','ESTADO'], as_index=False)['Emissão NMCOV (kg)'].sum().rename(columns={'index_right':'grid_id'})
emissoes_com_coords = pd.merge(emissoes_agregadas, grid_base[['lat','lon']], left_on='grid_id', right_index=True)
emissoes_com_indice = emissoes_com_coords.set_index(['ESTADO','num_ano','lat','lon'])
data_array = xr.DataArray.from_series(emissoes_com_indice['Emissão NMCOV (kg)']).rename({'ESTADO':'estado','num_ano':'time'}).transpose('estado','time','lat','lon')
ds_emissoes_completo = xr.Dataset({'nmvoc_emissions': data_array}).fillna(0)

# -------------------------------------------------------------------------
# TENDÊNCIA POR PIXEL
mapa_tendencia = xr.apply_ufunc(
    calcular_tendencia_pixel, ds_emissoes_completo['nmvoc_emissions'],
    input_core_dims=[['time']], output_core_dims=[[]],
    vectorize=True,
    dask='parallelized',
    output_dtypes=[np.float64]
)

# -------------------------------------------------------------------------
# TENDÊNCIA GERAL POR ESTADO
tendencia_geral_estados_df = analisar_tendencia_nmvc(df, group_cols=['ESTADO'])

# -------------------------------------------------------------------------
# FUNÇÃO PARA PLOT FINAL POR ESTADO
def plotar_painel_estado(nome_estado, df_completo):
    """Cria o painel de visualização para um estado, com checagem de dados."""
    try:
        # Seleciona o DataArray de tendência para o estado
        if nome_estado not in ds_emissoes_completo['nmvoc_emissions'].estado.values:
            print(f"Aviso: {nome_estado} não existe no cubo de dados. Painel não gerado.")
            return

        tendencia_mapa_estado = mapa_tendencia.sel(estado=nome_estado)

        # Verifica se há dados numéricos para plotar
        if tendencia_mapa_estado.isnull().all():
            print(f"Aviso: {nome_estado} não possui dados de tendência válidos. Painel não gerado.")
            return

        # Informações de tendência geral do estado
        info_tendencia_geral = tendencia_geral_estados_df[tendencia_geral_estados_df['ESTADO'] == nome_estado]
        if info_tendencia_geral.empty:
            print(f"Aviso: Não há análise de tendência geral para '{nome_estado}'.")
            return
        info_tendencia_geral = info_tendencia_geral.iloc[0]

        # Criação da figura e eixos
        fig, (ax1, ax2) = plt.subplots(nrows=1, ncols=2, figsize=(15, 6.5), gridspec_kw={'width_ratios': [1, 1.1]})

        # --- MAPA DE TENDÊNCIAS ---
        cores = ['red', 'lightgray', 'blue']
        cmap_tendencia = ListedColormap(cores)
        estados.boundary.plot(ax=ax1, color='black', linewidth=0.7)
        tendencia_mapa_estado.plot.imshow(ax=ax1, cmap=cmap_tendencia, vmin=-1, vmax=1, add_colorbar=False)
        ax1.set_title(f'Tendência de Emissão por Pixel', fontsize=12, weight='bold')
        ax1.set_aspect('equal')
        ax1.axis('off')

        # Ajuste do zoom para o estado
        estado_geom = estados[estados['Estado'] == nome_estado]
        if not estado_geom.empty:
            minx, miny, maxx, maxy = estado_geom.total_bounds
            ax1.set_xlim(minx - 1, maxx + 1)
            ax1.set_ylim(miny - 1, maxy + 1)

        # Legenda
        legend_elements = [
            Patch(facecolor='blue', label='Aumento Significativo'),
            Patch(facecolor='red', label='Diminuição Significativa'),
            Patch(facecolor='lightgray', label=f'Sem Tendência (p≥{ALPHA})')
        ]
        ax1.legend(handles=legend_elements, loc='lower left', fontsize=9, title_fontsize=10, title="Legenda")

        # --- SÉRIE HISTÓRICA ---
        df_estado = df_completo[df_completo['ESTADO'] == nome_estado]
        plot_serie_historica_no_eixo(df_estado, ax2, nome_estado)

        # Adiciona texto com tendência geral
        tendencia = info_tendencia_geral['tendência']
        p_valor = info_tendencia_geral['p-valor']
        ax2.text(0.97, 0.97, f'Tendência Geral: {tendencia}\n(p-valor: {p_valor:.3f})',
                 transform=ax2.transAxes, fontsize=10, verticalalignment='top', horizontalalignment='right',
                 bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        # Título geral e salvamento
        fig.suptitle(f'Análise de Tendência de Emissões de NMVOC - {nome_estado}', fontsize=16, weight='bold')
        plt.tight_layout(rect=[0, 0, 1, 0.94])

        filepath = os.path.join(figures_path_paineis, f'painel_tendencia_{nome_estado}.png')
        plt.savefig(filepath, dpi=300)
        plt.close(fig)
        print(f"-> Painel para {nome_estado} salvo com sucesso.")

    except Exception as e:
        print(f"-> ERRO ao gerar painel para '{nome_estado}': {e}")


# -------------------------------------------------------------------------
# LOOP PARA TODOS OS ESTADOS PRESENTES
for estado in df['ESTADO'].dropna().unique():
    plotar_painel_estado(estado, df)

print("Processo concluído!")
