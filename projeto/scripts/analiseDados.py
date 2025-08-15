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

def plot_emissao(df, path, coluna=None):
    """
    Plota gráficos de emissões de NMCOV (kg) por ano com intervalo de confiança.
    
    Parâmetros:
    - df: DataFrame com as colunas:
        'num_ano', 'Emissão NMCOV (kg)', 
        'Emissão NMCOV CI_lower (kg)', 'Emissão NMCOV CI_upper (kg)'
    - coluna: str (opcional) -> coluna para separar gráficos (ex: 'estado')
    
    Exemplos:
    plot_emissao(df)              # Brasil inteiro
    plot_emissao(df, coluna='UF') # Gráfico por estado
    """
    
    def plot_um(df_plot, titulo_extra):
        """Plota um gráfico para um conjunto filtrado."""
        # Agrupa por ano
        df_agg = df_plot.groupby('num_ano', as_index=False).agg({
            'Emissão NMCOV (kg)': 'sum',
            'Emissão NMCOV CI_lower (kg)': 'sum',
            'Emissão NMCOV CI_upper (kg)': 'sum'
        })
        
        plt.figure(figsize=(10, 6))
        plt.plot(df_agg['num_ano'], df_agg['Emissão NMCOV (kg)'], 
                 marker='o', linestyle='-', label='Emissão de NMCOV (kg)')
        plt.fill_between(df_agg['num_ano'],
                         df_agg['Emissão NMCOV CI_lower (kg)'],
                         df_agg['Emissão NMCOV CI_upper (kg)'],
                         color='b', alpha=0.2, label='Margem de Erro (IC)')
        plt.title(f'Emissão de NMCOV (kg) por Ano - {titulo_extra}')
        plt.xlabel('Ano')
        plt.ylabel('Emissão de NMCOV (kg)')
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
            
def analisar_tendencia_nmvc(df, group_cols):
    resultados = []

    for grupo_valores, grupo_df in df.groupby(group_cols):
        if isinstance(grupo_valores, str):
            grupo_valores = (grupo_valores,)

        grupo_dict = dict(zip(group_cols, grupo_valores))
        print(f"\nAnalisando: {grupo_dict}")

        serie_anual = grupo_df.groupby('num_ano')["Emissão NMCOV (kg)"].sum().sort_index()

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

import pandas as pd
import numpy as np
import pymannkendall as mk
from tqdm.auto import tqdm # Para uma barra de progresso bonita e informativa

def analisar_tendencia_nmvc_aprimorada(df, group_cols):
    """
    Analisa a tendência de emissões de NMCOV usando o teste de Mann-Kendall 
    para diferentes grupos de dados.

    Parâmetros:
    - df: DataFrame com as colunas 'num_ano' e 'Emissão NMCOV (kg)'.
    - group_cols: Lista de colunas para agrupar (ex: ['ESTADO'] ou ['ESTADO', 'MUNICIPIO']).

    Retorna:
    - Um DataFrame com os resultados detalhados do teste para cada grupo.
    """
    resultados = []
    
    # Prepara os grupos para iterar com uma barra de progresso
    grupos_df = df.groupby(group_cols)
    
    # A barra de progresso (tqdm) é ótima para saber o status da análise
    for grupo_valores, grupo_df in tqdm(grupos_df, desc="Analisando tendências"):
        
        # Garante que 'grupo_valores' seja sempre uma tupla para consistência
        if not isinstance(grupo_valores, tuple):
            grupo_valores = (grupo_valores,)

        # Cria um dicionário com os identificadores do grupo (ex: {'ESTADO': 'SAO PAULO'})
        grupo_dict = dict(zip(group_cols, grupo_valores))
        
        # Prepara a série temporal: soma as emissões por ano e ordena
        serie_anual = grupo_df.groupby('num_ano')["Emissão NMCOV (kg)"].sum().sort_index()

        # --- Validação dos dados antes do teste ---
        # O teste não funciona com poucos dados, então definimos um dicionário de falha
        resultado_falha = {
            **grupo_dict, 'tendência': 'dados insuficientes', 'p-valor': np.nan, 
            'z': np.nan, 'Tau': np.nan, 'slope': np.nan, 'intercept': np.nan
        }

        # Pula para o próximo grupo se não houver dados suficientes
        if len(serie_anual) < 3:
            # print(f"-> {grupo_dict}: Dados insuficientes ({len(serie_anual)} anos). Pulando.")
            resultados.append(resultado_falha)
            continue

        # --- Execução do Teste ---
        try:
            resultado_mk = mk.original_test(serie_anual)

            # Adiciona os resultados bem-sucedidos à lista
            resultados.append({
                **grupo_dict,
                'tendência': resultado_mk.trend,
                'p-valor': resultado_mk.p,
                'z': resultado_mk.z,
                'Tau': resultado_mk.Tau,
                'slope': resultado_mk.slope,
                'intercept': resultado_mk.intercept
            })
            
        except Exception as e:
            # Em caso de erro durante o teste, registra a falha
            print(f"-> Erro na análise para {grupo_dict}: {e}")
            resultados.append(resultado_falha)

    return pd.DataFrame(resultados)

# --- EXEMPLO DE USO ---

