import xarray as xar
import thermofeel as tf
import pandas as pd
import pdb
import matplotlib.pyplot as plt

def transform_csv_to_xarray(filename):
    df = pd.read_csv(filename, parse_dates=['time'])


    ds = df.to_xarray()


    ds.coords['time'] = ds['time']

    dim_list = ['index', 'lat', 'lon', 'time']

    var_list = [key for key in ds.variables.keys() if key not in dim_list]

    for var in var_list:
        ds[var] = ('time', ds[var].values)

    return ds

future_experiment_name='ssp585'

hist_tasmax_ds = transform_csv_to_xarray('tasmax_NASA_NEX_GDDP_CMIP6.EC-Earth3.historical.day.NASA_NEX_50.73_356.47_JJA.csv')

ssp_tasmax_ds = transform_csv_to_xarray('tasmax_NASA_NEX_GDDP_CMIP6.EC-Earth3.ssp585.day.NASA_NEX_50.73_356.47_JJA.csv')

hist_hurs_ds = transform_csv_to_xarray('hurs_NASA_NEX_GDDP_CMIP6.EC-Earth3.historical.day.NASA_NEX_50.73_356.47_JJA.csv')

ssp_hurs_ds = transform_csv_to_xarray('hurs_NASA_NEX_GDDP_CMIP6.EC-Earth3.ssp585.day.NASA_NEX_50.73_356.47_JJA.csv')


hist_wbt = tf.calculate_wbt(hist_tasmax_ds['tasmax']-273., hist_hurs_ds['hurs'])
ssp_wbt = tf.calculate_wbt(ssp_tasmax_ds['tasmax']-273., ssp_hurs_ds['hurs'])


binnumber = 50

plt.figure()

plt.hist(hist_tasmax_ds['tasmax']-273., bins=binnumber, density=True, histtype='step', label='historical')
plt.hist(ssp_tasmax_ds['tasmax']-273., bins=binnumber, density=True, histtype='step', label=future_experiment_name)
plt.xlabel('T values (Celcius)')
plt.ylabel('Density')
plt.legend()

plt.figure()

plt.hist(hist_hurs_ds['hurs'], bins=binnumber, density=True, histtype='step', label='historical')
plt.hist(ssp_hurs_ds['hurs'], bins=binnumber, density=True, histtype='step', label=future_experiment_name)
plt.xlabel('RH values (Percent)')
plt.ylabel('Density')
plt.legend()

plt.figure()


plt.hist(hist_wbt, bins=binnumber, density=True, histtype='step', label='historical')
plt.hist(ssp_wbt, bins=binnumber, density=True, histtype='step', label=future_experiment_name)
plt.xlabel('WBT values (Celcius)')
plt.ylabel('Density')
plt.legend()