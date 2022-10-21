import intake
import matplotlib.pyplot as plt
import numpy as np
import s3fs
import xarray as xar
import pdb
from siphon.catalog import TDSCatalog
from dask.distributed import Client


if __name__=="__main__":
    client = Client(threads_per_worker=8, n_workers=1)

    model_to_search = 'EC-Earth3'
    method_to_search = 'GARD-SV'
    experiment_to_search = 'historical'
    variable_to_search = 'tasmax'
    ensemble_id = 'r1i1p1f1'

    s3_sub_path=f'{model_to_search}/{experiment_to_search}/{ensemble_id}/{variable_to_search}/'

    # s3_file_path = f'{variable_to_search}_day_{model_to_search}_{experiment_to_search}_{ensemble_id}_{grid_type}_{year_to_search}.nc'

    s3_path = f's3://nex-gddp-cmip6/NEX-GDDP-CMIP6/{s3_sub_path}' #path to PUBLIC bucket 
    thredds_base_path = 'https://ds.nccs.nasa.gov/thredds/dodsC/AMES/NEX/GDDP-CMIP6/'

    s3=s3fs.S3FileSystem(anon=True) #initialise the filesystem, including the anon=True option that can be used for public buckets

    available_files = s3.ls(f'{s3_path}')

    available_grids = np.unique([file_str.split('_')[-2] for file_str in available_files])

    available_years = np.unique([file_str.split('_')[-1].split('.nc')[0] for file_str in available_files])

    available_years=[1950,1951]

    if len(available_grids)==1:
        grid_to_use=available_grids[0]
    else:
        raise NotImplementedError(f'Multiple grids available: {available_grids}. Please choose one.')

    files=[f'{thredds_base_path}{model_to_search}/{experiment_to_search}/{ensemble_id}/{variable_to_search}/{variable_to_search}_day_{model_to_search}_{experiment_to_search}_{ensemble_id}_{grid_to_use}_{year_to_search}.nc' for year_to_search in available_years]

    dataset = xar.open_mfdataset(files, chunks={'time': '100MB'})


    # cat = TDSCatalog('http://ds.nccs.nasa.gov/thredds/catalog/AMES/NEX/GDDP-CMIP6/catalog.xml')

    #Let's now specifiy the location of the place I want to study:

    latitude_of_location = 50.7260
    longitude_of_location = 360.-3.5275
    season_to_study = 'JJA'

    #Loop over each dataset in my list to find a timeseries at that location, subset it for summer, and write it out to a csv file.

    ds = dataset #open the relevant dataset

    exp_id_of_ds = experiment_to_search

    print('Subsetting by location.')
    #Actually subset the data based on location and ensemble member
    ds_at_location = ds[variable_to_search].sel(lat=latitude_of_location, lon=longitude_of_location, method='nearest')

    print('subsetting by time slice')
    #Specify the time periods I want to look at
    if exp_id_of_ds=='historical':
        #If using historical data then just look at whole thing
        time_slice = ds_at_location#.sel(time=slice('1950-01-01', '1999-12-31'))
    else:
        #If using a climate change scenario then look at the end of the century        
        time_slice = ds_at_location.sel(time=slice('2080-01-01', '2099-12-31'))

    print('Selecting summer only')
    #Select only the summer months (JJA in the northern hemisphere)
    time_slice_summer = time_slice.where(time_slice.time.dt.season==season_to_study, drop=True)


    dset_name=f'{variable_to_search}_day_{model_to_search}_{experiment_to_search}_{ensemble_id}_{grid_to_use}_{available_years[0]}_{available_years[-1]}'

    #Specify the name of my output file
    filename_out = f'{dset_name}_{latitude_of_location:.2f}_{longitude_of_location:.2f}_{season_to_study}.csv'

    print('Computing')
    final_data = time_slice_summer.compute()

    print('writing file to csv')
    #Send it to a csv file
    final_data.to_dataframe().to_csv(filename_out)