import intake
import matplotlib.pyplot as plt
import numpy as np
import s3fs
import xarray as xar
import pdb
from siphon.catalog import TDSCatalog
from dask.distributed import Client
import dask
import os

def get_file(fs_in, full_path_in, local_full_path_in):
    fs_in.get(full_path_in, local_full_path_in)

if __name__=="__main__":

    client = Client(threads_per_worker=8, n_workers=1)

    for variable_to_search in ['tasmax', 'hurs']:
        for experiment_to_search in ['historical', 'ssp585']:

            model_to_search = 'EC-Earth3'
            # experiment_to_search = 'historical'
            # variable_to_search = 'tasmax'
            ensemble_id = 'r1i1p1f1'

            s3_sub_path=f'{model_to_search}/{experiment_to_search}/{ensemble_id}/{variable_to_search}/'

            # s3_file_path = f'{variable_to_search}_day_{model_to_search}_{experiment_to_search}_{ensemble_id}_{grid_type}_{year_to_search}.nc'

            s3_path = f's3://nex-gddp-cmip6/NEX-GDDP-CMIP6/{s3_sub_path}' #path to PUBLIC bucket 

            local_file_path='/Users/stephenthomson/Desktop/nasa_stuff/'

            s3=s3fs.S3FileSystem(anon=True) #initialise the filesystem, including the anon=True option that can be used for public buckets

            available_files = s3.ls(f'{s3_path}')

            available_grids = np.unique([file_str.split('_')[-2] for file_str in available_files])

            available_years = np.unique([file_str.split('_')[-1].split('.nc')[0] for file_str in available_files])

            if experiment_to_search=='historical':
                # 1950-1999
                pdb.set_trace()
                available_years = [f'{num}' for num in range(1950,2000)]
            else:
                #end of century
                available_years = [f'{num}' for num in range(2080,2099)]

            if len(available_grids)==1:
                grid_to_use=available_grids[0]
            else:
                raise NotImplementedError(f'Multiple grids available: {available_grids}. Please choose one.')

            files=[f'{variable_to_search}_day_{model_to_search}_{experiment_to_search}_{ensemble_id}_{grid_to_use}_{year_to_search}.nc' for year_to_search in available_years]    

            files_to_download_dask = []

            for file_to_download in files:

                s3_file_to_get = f'{s3_path}{file_to_download}'
                local_file_location = f'{local_file_path}{file_to_download}'

                if not os.isfile(local_file_location):
                    files_to_download_dask.append(dask.delayed(get_file)(s3, s3_file_to_get, local_file_location))

            out = dask.compute(*files_to_download_dask)        