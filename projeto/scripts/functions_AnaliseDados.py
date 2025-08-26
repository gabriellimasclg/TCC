# -*- coding: utf-8 -*-
"""
Created on Thu Aug 14 09:18:30 2025

@author: glima
"""
import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np
import pymannkendall as mk
import xarray as xr
from shapely.geometry import box
import geopandas as gpd
from matplotlib.colors import LogNorm, Normalize, ListedColormap, BoundaryNorm
import matplotlib.gridspec as gridspec
from matplotlib.patches import Patch
from clean_text import clean_text

#%% plot de emissão por ano - pode ser no brasil ou por estado

def plot_emissao(df, path, coluna=None):
    """
    Plota gráficos de emissões de NMCOV (ton) por ano com intervalo de confiança.
    
    Parâmetros:
    - df: DataFrame com as colunas:
        'num_ano', 'Emissão NMCOV (ton)', 
        'Emissão NMCOV CI_lower (ton)', 'Emissão NMCOV CI_upper (ton)'
    - coluna: str (opcional) -> coluna para separar gráficos (ex: 'estado')
    
    Exemplos:
    plot_emissao(df)              # Brasil inteiro
    plot_emissao(df, coluna='UF') # Gráfico por estado
    """
    
    def plot_um(df_plot, titulo_extra):
        """Plota um gráfico para um conjunto filtrado."""
        # Agrupa por ano
        df_agg = df_plot.groupby('num_ano', as_index=False).agg({
            'Emissão NMCOV (ton)': 'sum',
            'Emissão NMCOV CI_lower (ton)': 'sum',
            'Emissão NMCOV CI_upper (ton)': 'sum'
        })
        
        plt.figure(figsize=(10, 6))
        plt.plot(df_agg['num_ano'], df_agg['Emissão NMCOV (ton)'], 
                 marker='o', linestyle='-', label='Emissão de NMCOV (ton)')
        plt.fill_between(df_agg['num_ano'],
                         df_agg['Emissão NMCOV CI_lower (ton)'],
                         df_agg['Emissão NMCOV CI_upper (ton)'],
                         color='b', alpha=0.2, label='Margem de Erro (IC)')
        plt.title(f'Emissão de NMCOV (ton) da Indústria Alimentícia - {titulo_extra}')
        plt.xlabel('Ano')
        plt.ylabel('Emissão de NMCOV (ton)')
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(path,f'Série Histórica de Emissão {titulo_extra}'))
        plt.show()
    
    if coluna is None:
        # Um único gráfico Brasil
        plot_um(df, "Brasil")
    else:
        # Um gráfico para cada valor único da coluna
        for valor in sorted(df[coluna].dropna().unique()):
            df_filtro = df[df[coluna] == valor]
            plot_um(df_filtro, f"{coluna}: {valor}")

#%% Emissões por estado

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch

def plot_emissoes_estado(df_final, figpath, top_n=None):
    """
    Plota gráfico de barras com IC em escala log,
    colorindo pelas tendências já existentes no df_final.
    """
    # Ordenar
    df_plot = df_final.sort_values('Emissão NMCOV (ton)', ascending=False)
    if top_n:
        df_plot = df_plot.head(top_n)

    # Cores pela tendência
    mapa_cores = {
        'increasing': '#0077b6',
        'decreasing': '#d7191c',
        'no trend': 'gray'
    }
    
    mapa_legenda = {
        'Aumento Significativo': '#0077b6',
        'Diminuição Significativa': '#d7191c',
        'Sem Tendência Significativa': 'gray'
    }
    
    cores = df_plot['tendência'].map(mapa_cores).fillna('lightgray')

    # Valores e erros
    valores = df_plot['Emissão NMCOV (ton)']
    erro_inferior = valores - df_plot['Emissão NMCOV CI_lower (ton)']
    erro_superior = df_plot['Emissão NMCOV CI_upper (ton)'] - valores
    erros = np.array([erro_inferior, erro_superior])

    # Plot
    plt.figure(figsize=(12,6))
    plt.bar(df_plot['ESTADO'], valores,
            yerr=erros, capsize=4, color=cores, alpha=0.85)

    plt.yscale("log")
    plt.ylabel("Emissões de NMCOV (ton) [escala log]")
    plt.title("Emissões Acumuladas de NMCOV da Indústria Alimentícia\nPor Estado Entre 2017 à 2024")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.grid(True, axis="y", which="both", linestyle="--", color="gray", alpha=0.5)
    plt.gca().set_axisbelow(True)  # garante que o grid fique atrás das barras


    # Legenda
    legenda = [Patch(color=v, label=k) for k,v in mapa_legenda.items()]
    plt.legend(handles=legenda, title="Tendência")
    
    plt.savefig(os.path.join(figpath,'EmissõesAcumuladasEstado.png'))
    plt.show()

#%% função de análise de tendência de emissões
            
def analisar_tendencia_nmvc(df, group_cols):
    resultados = []

    for grupo_valores, grupo_df in df.groupby(group_cols):
        if isinstance(grupo_valores, str):
            grupo_valores = (grupo_valores,)

        grupo_dict = dict(zip(group_cols, grupo_valores))
        print(f"\nAnalisando: {grupo_dict}")

        serie_anual = grupo_df.groupby('num_ano')["Emissão NMCOV (ton)"].sum().sort_index()

        try:
            resultado = mk.original_test(serie_anual)

            resultados.append({
                **grupo_dict,
                'tendência': resultado.trend,
                'p-valor': resultado.p,
                'z': resultado.z,
                'Tau': resultado.Tau,
                'slope': resultado.slope,
                'intercept': resultado.intercept
            })
            print(f"  Tendência: {resultado.trend}, p-valor: {resultado.p:.3f}")
        except Exception as e:
            print(f"  Erro na análise: {e}")
            resultados.append({
                **grupo_dict,
                'tendência': np.nan,
                'p-valor': np.nan,
                'z': np.nan,
                'Tau': np.nan,
                'slope': np.nan,
                'intercept': np.nan
            })

    return pd.DataFrame(resultados)
#%% Função de geração do cube-data

def criar_cubo_emissoes_geograficas(
    df_emissoes: pd.DataFrame,
    coluna_emissao: str,
    coluna_lat: str = 'LATITUDE',
    coluna_lon: str = 'LONGITUDE',
    coluna_ano: str = 'num_ano',
    coluna_estado: str = 'ESTADO',
    limites_grid: dict = {'xmin': -74, 'xmax': -34, 'ymin': -34, 'ymax': 6},
    resolucao: float = 0.5,
    crs: str = 'EPSG:4674',
    preencher_na_com: int = 0
) -> xr.Dataset:
    
    # --- 1. FUNÇÃO AUXILIAR PARA CRIAR A GRADE ---
    def _create_grid(res, xmin, xmax, ymin, ymax, crs_grid):
        x_coords = np.arange(xmin, xmax, res)
        y_coords = np.arange(ymin, ymax, res)
        grid_cells = [box(x, y, x + res, y + res) for x in x_coords for y in y_coords]
        grid = gpd.GeoDataFrame(geometry=grid_cells, crs=crs_grid)
        grid['lon'] = grid.geometry.centroid.x
        grid['lat'] = grid.geometry.centroid.y
        return grid

    # --- 2. PREPARAÇÃO E VALIDAÇÃO DOS DADOS ---
    print("Iniciando a criação do cubo de dados...")
    df_filtrado = df_emissoes.dropna(
        subset=[coluna_lat, coluna_lon, coluna_emissao]
    ).copy()

    gdf_emissoes = gpd.GeoDataFrame(
        df_filtrado,
        geometry=gpd.points_from_xy(
            df_filtrado[coluna_lon].astype(float),
            df_filtrado[coluna_lat].astype(float)
        ),
        crs=crs
    )

    # --- 3. CRIAÇÃO DA GRADE E JUNÇÃO ESPACIAL ---
    print("Criando grade de referência e realizando junção espacial...")
    grid_base = _create_grid(resolucao, **limites_grid, crs_grid=crs)

    # Checa se há pontos para processar
    if gdf_emissoes.empty:
        print("Atenção: O DataFrame de emissões está vazio após a filtragem. Retornando um cubo vazio.")
        # Retorna um cubo vazio, mas com a estrutura correta (ainda a ser implementado se necessário)
        # Por enquanto, vamos parar aqui para evitar erros. O ideal seria construir um cubo de zeros.
        return None # Ou uma estrutura de cubo vazia

    pontos_na_grade = gpd.sjoin(gdf_emissoes, grid_base, how="inner", predicate="within")

    # --- 4. AGREGAÇÃO DOS DADOS ---
    print("Agregando emissões por célula da grade, ano e estado...")
    emissoes_agregadas = (
        pontos_na_grade
        .groupby(['index_right', coluna_ano, coluna_estado], as_index=False)[coluna_emissao]
        .sum()
        .rename(columns={'index_right': 'grid_id'})
    )

    # --- 5. TRANSFORMAÇÃO ROBUSTA PARA O FORMATO XARRAY ---
    print("Estruturando os dados no formato xarray (método robusto)...")
    
    # Junta os dados agregados com as coordenadas da grade
    emissoes_com_coords = pd.merge(
        emissoes_agregadas,
        grid_base[['lat', 'lon']],
        left_on='grid_id',
        right_index=True
    )
    
    # Define o índice com as futuras dimensões do cubo
    emissoes_com_indice = emissoes_com_coords.set_index(
        [coluna_estado, coluna_ano, 'lat', 'lon']
    )[coluna_emissao]

    # Converte de Pandas Series para xarray DataArray (ainda pode ser esparso)
    data_array_esparso = xr.DataArray.from_series(emissoes_com_indice)

    # *** A MUDANÇA PRINCIPAL ESTÁ AQUI ***
    # Define TODAS as coordenadas que o cubo final DEVE ter
    todos_estados = df_emissoes[coluna_estado].unique()
    todos_anos = df_emissoes[coluna_ano].unique()
    todas_lats = grid_base['lat'].unique()
    todas_lons = grid_base['lon'].unique()

    # Usa .reindex() para garantir que o array tenha todas as coordenadas
    # Isso cria um cubo denso, preenchendo com NaN onde não há dados
    data_array_denso = data_array_esparso.reindex(
        {
            coluna_estado: todos_estados,
            coluna_ano: todos_anos,
            'lat': todas_lats,
            'lon': todas_lons
        }
    )

    # Renomeia as dimensões para o padrão desejado
    data_array_final = data_array_denso.rename({
        coluna_estado: 'estado',
        coluna_ano: 'time'
    })
    
    # Cria o Dataset final
    ds_emissoes = xr.Dataset({'emissions': data_array_final})
    
    # Preenche os valores ausentes (NaN) com o valor especificado (ex: 0)
    ds_emissoes = ds_emissoes.fillna(preencher_na_com)
    
    # A conversão explícita de tipo agora é uma garantia extra
    ds_emissoes = ds_emissoes.assign_coords(
        lon=ds_emissoes.coords['lon'].astype(float),
        lat=ds_emissoes.coords['lat'].astype(float)
    )

    return ds_emissoes

#%% Plotart mosaico emissões

def plotar_mosaico_emissoes(
    ds,
    data_var='emissions',
    titulo='Evolução Anual da Emissão Agregada no Brasil (kg)',
    cbar_label='Emissão Agregada (kg)',
    cmap='YlOrRd',
    scale='log',
    grid_cols=3,
    save_path=None
):
    """
    Cria e salva um mosaico de mapas de emissões anuais para o Brasil.

    Args:
        ds (xr.Dataset): O Dataset xarray contendo os dados de emissões com as
            dimensões ('estado', 'time', 'lat', 'lon').
        data_var (str, optional): Nome da variável de dados a ser plotada. 
            Padrão 'emissions'.
        titulo (str, optional): Título principal da figura.
        cbar_label (str, optional): Rótulo da barra de cores.
        cmap (str, optional): Mapa de cores (colormap) a ser usado. Padrão 'YlOrRd'.
        scale (str, optional): Escala da barra de cores ('log' ou 'linear'). Padrão 'log'.
        grid_cols (int, optional): Número de colunas no grid de subplots. Padrão 3.
        save_path (str, optional): Caminho completo (incluindo nome do arquivo e extensão,
            ex: 'figuras/meu_mapa.png') para salvar a figura. Se None, a figura
            apenas será exibida. Padrão None.

    Returns:
        matplotlib.figure.Figure: O objeto da figura criada.
        matplotlib.axes.Axes: O array de eixos (subplots) criados.
    """
    # --- 1. PREPARAÇÃO DOS DADOS ---
    print("Iniciando a criação do mosaico...")
    # Carregar os limites dos estados do Brasil (dentro da função para ser autossuficiente)
    try:
        url_estados = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
        estados = gpd.read_file(url_estados)
    except Exception as e:
        print(f"Não foi possível carregar o GeoJSON dos estados: {e}")
        return None, None
        
    # Extrai os limites geográficos do shapefile dos estados
    xmin, ymin, xmax, ymax = estados.total_bounds

    # Agrega os dados, somando as emissões de todos os estados para cada ponto e ano
    dados_agregados = ds[data_var].sum(dim='estado')
    anos = dados_agregados['time'].values

    # --- 2. CONFIGURAÇÃO DA ESCALA DE CORES ---
    vmax = dados_agregados.max().item()
    if scale == 'log':
        # Para escala log, o mínimo não pode ser zero. Encontramos o menor valor > 0.
        vmin = dados_agregados.where(dados_agregados > 0).min().item()
        if np.isnan(vmin) or vmin <= 0: # Caso não haja valores > 0
            vmin = 1e-9
        norm = LogNorm(vmin=vmin, vmax=vmax)
        cbar_label_final = f"{cbar_label} [Escala Log]"
    else: # Escala linear
        vmin = dados_agregados.min().item()
        norm = Normalize(vmin=vmin, vmax=vmax)
        cbar_label_final = f"{cbar_label} [Escala Linear]"

    # --- 3. CONFIGURAÇÃO DO GRID DE PLOTS ---
    num_plots = len(anos)
    grid_rows = int(np.ceil(num_plots / grid_cols))
    
    fig, axes = plt.subplots(
        nrows=grid_rows,
        ncols=grid_cols,
        figsize=(4 * grid_cols, 4.5 * grid_rows),
        constrained_layout=True
    )
    axes_flat = axes.flatten()

    # --- 4. LOOP PARA CRIAR CADA SUBPLOT ---
    print(f"Gerando {num_plots} mapas anuais...")
    for i, ano in enumerate(anos):
        ax = axes_flat[i]
        data_slice = dados_agregados.sel(time=ano)
        
        # Plotar limites dos estados
        estados.boundary.plot(ax=ax, linewidth=0.6, color='gray', zorder=2)
        
        # Plotar heatmap de emissões
        mappable = data_slice.plot(
            ax=ax,
            cmap=cmap,
            add_colorbar=False,
            norm=norm,
            zorder=1
        )
        
        # Customização do subplot
        ax.set_title(str(ano), fontsize=14)
        ax.set_aspect('equal')
        ax.set_xlim(xmin, xmax)
        ax.set_ylim(ymin, ymax)
        ax.set_xlabel('')
        ax.set_ylabel('')
        ax.tick_params(axis='both', which='both', bottom=False, top=False, left=False, right=False, labelbottom=False, labelleft=False)
        for spine in ax.spines.values():
            spine.set_visible(False)

    # Esconder eixos não utilizados
    for i in range(num_plots, len(axes_flat)):
        axes_flat[i].set_visible(False)

    # --- 5. FINALIZAÇÃO DA FIGURA ---
    fig.suptitle(titulo, fontsize=22, weight='bold', y=1.03)
    
    # Barra de cores compartilhada
    cbar = fig.colorbar(
        mappable,
        ax=axes.ravel().tolist(),
        orientation='horizontal',
        location='bottom',
        shrink=0.6,
        pad=0.05,
        aspect=40
    )
    cbar.set_label(cbar_label_final, fontsize=12)

    # Salvar a figura, se um caminho foi fornecido
    if save_path:
        # Garante que o diretório de destino exista
        save_dir = os.path.dirname(save_path)
        if save_dir and not os.path.exists(save_dir):
            os.makedirs(save_dir)
        
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"Figura salva em: {save_path}")
    
    plt.show()
    print("Processo concluído.")
    
    return fig, axes

#%% Função de análise de tendência por pixel

def analisar_tendencia_pixel(ds_estado, alpha=0.05, data_var='emissions'):
    """
    Aplica o teste de Mann-Kendall a cada pixel de um DataArray xarray.
    Garante a diferenciação entre 'sem tendência' e 'sem dados'.
    """
    def mk_test_wrapper(timeseries_1d):
        if np.all(np.isnan(timeseries_1d)) or np.all(timeseries_1d == 0):
            return "no data", np.nan
        try:
            resultado = mk.original_test(timeseries_1d)
            return resultado.trend, resultado.p
        except Exception:
            return "no data", np.nan

    tendencia_str, p_valor = xr.apply_ufunc(
        mk_test_wrapper,
        ds_estado[data_var],
        input_core_dims=[['time']],
        output_core_dims=[[], []],
        exclude_dims=set(('time',)),
        vectorize=True,
        dask="parallelized",
        output_dtypes=[object, float]
    )
    
    # Mapeia resultados de string para número (1, -1, 0, ou NaN para 'no data')
    tendencia_num = xr.full_like(tendencia_str, fill_value=np.nan, dtype=float)
    tendencia_num = tendencia_num.where(tendencia_str != 'increasing', 1)
    tendencia_num = tendencia_num.where(tendencia_str != 'decreasing', -1)
    tendencia_num = tendencia_num.where(tendencia_str != 'no trend', 0)
    
    # Aplica o filtro de significância (p < alpha).
    # Onde a tendência não é significativa, o valor vira 0.
    tendencia_com_filtro_p = tendencia_num.where(p_valor < alpha, 0)
    
    tendencia_significativa = tendencia_com_filtro_p.where(tendencia_str != 'no data')
    
    #filtro para onde n tem dado
    tendencia_final = tendencia_significativa.fillna(-999)

    return xr.Dataset({'tendencia': tendencia_final, 'p_valor': p_valor})




#%% Função que plota o mosaico por estado

def plotar_mosaico_estado(df, ds, tendencia_uf_df, estado_alvo, save_path=None):
    """
    Gera um mosaico completo para um único estado.
    Escala log, layout compacto.
    """
    # Geração da figura
    fig = plt.figure(figsize=(18, 9))
    
    # Ajustar a proporção de altura para diminuir o espaço do texto inferior
    gs = gridspec.GridSpec(2, 2, height_ratios=[8, 1], figure=fig)
    
    fig.suptitle(f"Emissões de NMVOC da Indústria Alimentícia - {estado_alvo}", fontsize=20, weight='bold', y=0.98)

    # GRÁFICO DE BARRAS
    ax1 = fig.add_subplot(gs[0, 0])
    df_estado = df[df['ESTADO'] == estado_alvo]

    colunas_emissao = 'Emissão NMCOV (ton)'
    coluna_ic_lower = 'Emissão NMCOV CI_lower (ton)'
    coluna_ic_upper = 'Emissão NMCOV CI_upper (ton)'
    has_ic = all(col in df_estado.columns for col in [coluna_ic_lower, coluna_ic_upper])

    if has_ic:
        df_agg = df_estado.groupby('num_ano', as_index=False).agg({
            colunas_emissao: 'sum',
            coluna_ic_lower: 'sum',
            coluna_ic_upper: 'sum'
        })
    else:
        df_agg = df_estado.groupby('num_ano', as_index=False).agg({
            colunas_emissao: 'sum'
        })
        print("Aviso: Colunas de Intervalo de Confiança não encontradas. O gráfico será gerado sem a margem de erro.")

    ax1.bar(df_agg['num_ano'], df_agg[colunas_emissao], color='skyblue', edgecolor='black', zorder=2)

    if has_ic:
        lower_error = df_agg[colunas_emissao] - df_agg[coluna_ic_lower]
        upper_error = df_agg[coluna_ic_upper] - df_agg[colunas_emissao]
        asymmetric_error = [lower_error, upper_error]
        ax1.errorbar(df_agg['num_ano'], df_agg[colunas_emissao], yerr=asymmetric_error,
                     fmt='none', capsize=5, color='black', elinewidth=1.5, zorder=3)

    anos = df_agg['num_ano']
    emissoes = df_agg[colunas_emissao]
    slope, intercept = np.polyfit(anos, emissoes, 1)
    trend_line = slope * anos + intercept
    ax1.plot(anos, trend_line, color='red', linestyle='--', linewidth=2, label='Linha de Tendência', zorder=4)

    ax1.set_title('Emissão Anual Total', fontsize=14)
    ax1.set_ylabel('Emissão (ton)')
    ax1.set_xlabel('Ano')
    ax1.grid(axis='y', linestyle='--', alpha=0.7, zorder=0)
    ax1.set_xticks(df_agg['num_ano'])
    ax1.tick_params(axis='x', rotation=45)
    ax1.legend()
    
    # Colocar o eixo Y em escala logarítmica
    ax1.set_yscale('log')

    # MAPA DE TENDÊNCIA
    ax2 = fig.add_subplot(gs[0, 1])
    try:
        url_estados = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
        gdf_estados = gpd.read_file(url_estados)
        nome_limpo_alvo = clean_text(estado_alvo)
        gdf_estado_alvo = gdf_estados[gdf_estados['name'].apply(clean_text) == nome_limpo_alvo]
        if gdf_estado_alvo.empty: raise ValueError(f"Não foi possível encontrar o estado '{estado_alvo}' no GeoJSON.")
    except Exception as e:
        ax2.text(0.5, 0.5, f"Erro ao carregar shapefile:\n{e}", ha='center', va='center', wrap=True)
        return fig
    ds_estado = ds.sel(estado=estado_alvo)
    ds_tendencia_pixel = analisar_tendencia_pixel(ds_estado)
    colors = ['#ffffff', '#d7191c', '#a9a9a9', '#0077b6']
    bounds = [-1000, -998, -0.5, 0.5, 1.5]
    cmap = ListedColormap(colors)
    norm = BoundaryNorm(bounds, cmap.N)
    ds_tendencia_pixel['tendencia'].plot.imshow(ax=ax2, cmap=cmap, norm=norm, add_colorbar=False, alpha=0.8)
    gdf_estado_alvo.boundary.plot(ax=ax2, linewidth=1.5, color='black', zorder=10)
    xmin, ymin, xmax, ymax = gdf_estado_alvo.total_bounds
    ax2.set_xlim(xmin - 1, xmax + 1); ax2.set_ylim(ymin - 1, ymax + 1)
    ax2.set_title('Tendência de Emissão por Pixel (p < 0.05)', fontsize=14)
    ax2.set_xlabel('Longitude'); ax2.set_ylabel('Latitude'); ax2.set_aspect('equal')
    legend_elements = [
        Patch(facecolor='#0077b6', edgecolor='black', label='Aumento Significativo'),
        Patch(facecolor='#d7191c', edgecolor='black', label='Diminuição Significativa'),
        Patch(facecolor='#a9a9a9', edgecolor='black', label='Sem Tendência Significativa'),
        Patch(facecolor='#ffffff', edgecolor='black', label='Sem Emissão / Dados')
    ]
    ax2.legend(handles=legend_elements, loc='best', fontsize='medium')

    # Texto com tendência geral
    ax_text = fig.add_subplot(gs[1, :])
    ax_text.axis("off")
    try:
        linha = tendencia_uf_df[tendencia_uf_df['ESTADO'] == estado_alvo].iloc[0]
        traducoes = {'increasing': 'Aumento Significativo', 'decreasing': 'Diminuição Significativa', 'no trend': 'Sem Tendência'}
        tendencia_original = str(linha['tendência'])
        tendencia = traducoes.get(tendencia_original, tendencia_original.title())
        p_valor = linha['p-valor']
        frase = f"Tendência Geral para o Estado: {tendencia} (p-valor = {p_valor:.3f})"
    except (IndexError, KeyError):
        frase = "Tendência Geral para o Estado: Dados não encontrados."
    ax_text.text(0.5, 0.5, frase, ha='center', va='center', fontsize=18)

    # Ajuste das margens e salvar figura
    plt.tight_layout(rect=[0, 0.01, 1, 0.97])
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
    
    return fig