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


