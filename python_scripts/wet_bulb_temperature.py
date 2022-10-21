import xarray as xar
import thermofeel as tf
import pandas as pd
import pdb
import matplotlib.pyplot as plt
import numpy as np

def transform_csv_to_xarray(filename):
    df = pd.read_csv(filename, parse_dates=['time'])


    ds = df.to_xarray()


    ds.coords['time'] = ds['time']

    dim_list = ['index', 'lat', 'lon', 'time']

    var_list = [key for key in ds.variables.keys() if key not in dim_list]

    for var in var_list:
        ds[var] = ('time', ds[var].values)

    return ds

latitude_of_location = 40.7128
longitude_of_location = 360.-74.006

future_experiment_name='ssp585'

hist_tasmax_ds = transform_csv_to_xarray(f'tasmax_NASA_NEX_GDDP_CMIP6.EC-Earth3.historical.day.NASA_NEX_{latitude_of_location:.2f}_{longitude_of_location:.2f}_JJA.csv')

ssp_tasmax_ds = transform_csv_to_xarray(f'tasmax_NASA_NEX_GDDP_CMIP6.EC-Earth3.{future_experiment_name}.day.NASA_NEX_{latitude_of_location:.2f}_{longitude_of_location:.2f}_JJA.csv')

hist_hurs_ds = transform_csv_to_xarray(f'hurs_NASA_NEX_GDDP_CMIP6.EC-Earth3.historical.day.NASA_NEX_{latitude_of_location:.2f}_{longitude_of_location:.2f}_JJA.csv')

ssp_hurs_ds = transform_csv_to_xarray(f'hurs_NASA_NEX_GDDP_CMIP6.EC-Earth3.{future_experiment_name}.day.NASA_NEX_{latitude_of_location:.2f}_{longitude_of_location:.2f}_JJA.csv')


hist_wbt = tf.calculate_wbt(hist_tasmax_ds['tasmax']-273., hist_hurs_ds['hurs'])
ssp_wbt = tf.calculate_wbt(ssp_tasmax_ds['tasmax']-273., ssp_hurs_ds['hurs'])

hist_wbgt = tf.calculate_wbgts(hist_tasmax_ds['tasmax'])
ssp_wbgt = tf.calculate_wbgts(ssp_tasmax_ds['tasmax'])

binnumber = 50

#Histogram for Temperature
plt.figure()
plt.hist(hist_tasmax_ds['tasmax']-273., bins=binnumber, density=True, histtype='step', label='historical')
plt.hist(ssp_tasmax_ds['tasmax']-273., bins=binnumber, density=True, histtype='step', label=future_experiment_name)
plt.xlabel('T values (Celcius)')
plt.ylabel('Density')
plt.legend()


#Histogram for RH
plt.figure()
plt.hist(hist_hurs_ds['hurs'], bins=binnumber, density=True, histtype='step', label='historical')
plt.hist(ssp_hurs_ds['hurs'], bins=binnumber, density=True, histtype='step', label=future_experiment_name)
plt.xlabel('RH values (Percent)')
plt.ylabel('Density')
plt.legend()

#Histogram for WBT
plt.figure()
hist_counts, hist_bins, hist_bars = plt.hist(hist_wbt, bins=binnumber, density=True, histtype='step', label='historical')
ssp_counts, ssp_bins, ssp_bars = plt.hist(ssp_wbt, bins=binnumber, density=True, histtype='step', label=future_experiment_name)
plt.xlabel('WBT values (Celcius)')
plt.ylabel('Density')
plt.legend()


#Cumulative Histogram for WBT
plt.figure()
hist_counts, hist_bins, hist_bars = plt.hist(hist_wbt, bins=binnumber, density=True, histtype='step', label='historical', cumulative=True)
ssp_counts, ssp_bins, ssp_bars = plt.hist(ssp_wbt, bins=binnumber, density=True, histtype='step', label=future_experiment_name, cumulative=True)
plt.xlabel('WBT values (Celcius)')
plt.ylabel('Density')
plt.legend()

hist_bins_plot = [0.5*(hist_bins[idx]+hist_bins[idx+1]) for idx in range(len(hist_counts))]
ssp_bins_plot = [0.5*(ssp_bins[idx]+ssp_bins[idx+1]) for idx in range(len(ssp_counts))]

hist_idx_gt_35 = np.where(np.asarray(hist_bins_plot)>=35)[0][0]
hist_percentage_of_days_with_wbt_gt_35 = (1.-hist_counts[hist_idx_gt_35])*100.

ssp_idx_gt_35 = np.where(np.asarray(ssp_bins_plot)>=35)[0][0]
ssp_percentage_of_days_with_wbt_gt_35 = (1.-ssp_counts[ssp_idx_gt_35])*100.

# This is a pretty crude measure!!
print(f'In hist period, percentage of JJA days with WBT gt 35C = {hist_percentage_of_days_with_wbt_gt_35:.2f}%.')
print(f'In ssp end of C period, percentage of JJA days with WBT gt 35C = {ssp_percentage_of_days_with_wbt_gt_35:.2f}%.')

#Histogram for WBGT
plt.figure()
plt.hist(hist_wbgt, bins=binnumber, density=True, histtype='step', label='historical')
plt.hist(ssp_wbgt, bins=binnumber, density=True, histtype='step', label=future_experiment_name)
plt.xlabel('WBGT values (Celsius)')
plt.ylabel('Density')
plt.legend()


#Cumulative Histogram for WBGT
plt.figure()
plt.hist(hist_wbgt, bins=binnumber, density=True, histtype='step', label='historical', cumulative=True)
plt.hist(ssp_wbgt, bins=binnumber, density=True, histtype='step', label=future_experiment_name, cumulative=True)
plt.xlabel('WBGT values (Celsius)')
plt.ylabel('Density')
plt.legend()

