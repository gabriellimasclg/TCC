# -*- coding: utf-8 -*-
"""
Created on Tue Jun 17 15:06:48 2025

@author: glima
"""

import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.interpolate import griddata

# Determine dimension sizes
nt = 20
nsite_id = 200

# Create dummy coordinates
times = pd.date_range("2000-01-01", "2018-01-01", periods = nt)
site_ids = np.linspace(0, nsite_id-1, nsite_id)
data = np.random.rand(nsite_id, nt)
lats = np.linspace(50, 60, nsite_id)
lons = np.linspace(20, 10, nsite_id)
np.random.shuffle(lats)
np.random.shuffle(lons)

# Create dummy dataset
ds = xr.Dataset({"data": (["site_id", "time"],data), "lat": (["site_id"], lats), "lon": (["site_id"], lons)}, coords = {"time": times, "site_id": site_ids})

print(ds)

#Then for your first question, you can select data by lat/lon "slices" like this:

# Select data by lat/lon
ds_selected = ds.where((ds.lat > 51) &
                       (ds.lat < 59) &
                       (ds.lon > 11) &
                       (ds.lon < 19), drop = True)

# ADICIONE ESSAS LINHAS PARA DIAGNOSTICAR
print("--- Diagnóstico ---")
print("Conteúdo do ds_selected:")
print(ds_selected)
print("\nNúmero de sites selecionados:", ds_selected.site_id.size)
print("---------------------\n")

test = ds_selected.set_index(site_id=("lat", "lon")).unstack("site_id")
test["data"].isel(time=0).plot()
#======================= nao estudei a partir daq abaixo
# Define function to interpolate sampled data to grid.
def interp_to_grid(u, xc, yc, new_lats, new_lons):
    new_points = np.stack(np.meshgrid(new_lats, new_lons), axis = 2).reshape((new_lats.size * new_lons.size, 2))
    z = griddata((xc, yc), u, (new_points[:,1], new_points[:,0]), method = 'nearest', fill_value = np.nan)
    out = z.reshape((new_lats.size, new_lons.size), order = "F")
    return out 

# Create chunks (you'd probably want to write `ds_selected` to disk 
# first using `.to_netcdf()` and then load it from there as a `dask.array`).
ds_selected = ds_selected.chunk({"site_id": -1, "time": 10})

values = ds_selected.data
lons = ds_selected.lon
lats = ds_selected.lat

# Create some dummy grid on which to interpolate.
_new_lats = np.linspace(ds_selected.lat.min(), ds_selected.lat.max(), 120)
_new_lons = np.linspace(ds_selected.lon.min(), ds_selected.lon.max(), 90)
new_lons = xr.DataArray(_new_lons, dims = "lon", coords = {"lon": _new_lons})
new_lats = xr.DataArray(_new_lats, dims = "lat", coords = {"lat": _new_lats})

# Vectorize the `interp_to_grid` function.
gridded_ds = xr.apply_ufunc(interp_to_grid,
                     values, lons, lats, new_lats, new_lons,
                     vectorize = True,
                     dask = "parallelized",
                     input_core_dims = [['site_id'],['site_id'],['site_id'],["lat"],["lon"]],
                     output_core_dims = [['lat', 'lon']],
                     )

# Create a plot
fig = plt.figure()
ax = fig.gca()
gridded_ds.isel(time=0).plot(ax = ax)
ax.scatter(lons, lats, c = values.isel(time=0), edgecolors="white")