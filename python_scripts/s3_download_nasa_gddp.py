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
import subprocess
import shutil

season_to_study='JJA'

def sel_summer(ds):

    dsnew = ds.where(ds.time.dt.season==season_to_study, drop=True)

    return dsnew

def get_file(fs_in, full_path_in, local_full_path_in):
    fs_in.get(full_path_in, local_full_path_in)


def create_s3_zarr_filename(bucket_name, local_file_to_upload):

    zarr_name = local_file_to_upload.split('/')[-1]
    outname = f's3://{bucket_name}/nasa_nex_downscaling/{zarr_name}'

    return outname

def upload_zarr_to_s3(bucket_name, local_file_to_upload, s3_filename):

    command = f'aws s3 cp {local_file_to_upload} {s3_filename} --recursive'
    aws_command_out = subprocess.run([command], shell=True, capture_output=True)
    print(f'{aws_command_out.stdout}, {aws_command_out.stderr}')


if __name__=="__main__":

    client = Client(threads_per_worker=8, n_workers=1)

    bucket_name='reanalysis-data'

    for model_to_search in ['EC-Earth3']:
        for variable_to_search in ['tasmax', 'hurs']:
            print(variable_to_search)
            for experiment_to_search in ['historical', 'ssp585']:
                print(experiment_to_search)
                ensemble_id = 'r1i1p1f1'

                s3_sub_path=f'{model_to_search}/{experiment_to_search}/{ensemble_id}/{variable_to_search}/'

                # s3_file_path = f'{variable_to_search}_day_{model_to_search}_{experiment_to_search}_{ensemble_id}_{grid_type}_{year_to_search}.nc'

                s3_path = f's3://nex-gddp-cmip6/NEX-GDDP-CMIP6/{s3_sub_path}' #path to PUBLIC bucket 

                # local_file_path='/Users/stephenthomson/Desktop/nasa_stuff/'
                local_file_path='/home/ubuntu/nasa_nex/'

                s3=s3fs.S3FileSystem(anon=True) #initialise the filesystem, including the anon=True option that can be used for public buckets

                available_files = s3.ls(f'{s3_path}')

                available_grids = np.unique([file_str.split('_')[-2] for file_str in available_files])

                available_years = np.unique([file_str.split('_')[-1].split('.nc')[0] for file_str in available_files])

                if experiment_to_search=='historical':
                    # 1950-1999
                    available_years = [f'{num}' for num in range(1950,2000)]
                    # available_years = [f'{num}' for num in range(1950,1955)]                
                else:
                    #end of century
                    available_years = [f'{num}' for num in range(2080,2099)]

                if len(available_grids)==1:
                    grid_to_use=available_grids[0]
                else:
                    raise NotImplementedError(f'Multiple grids available: {available_grids}. Please choose one.')

                files=[f'{variable_to_search}_day_{model_to_search}_{experiment_to_search}_{ensemble_id}_{grid_to_use}_{year_to_search}.nc' for year_to_search in available_years]    


                out_filename = f'{local_file_path}{variable_to_search}_{experiment_to_search}_{model_to_search}_{available_years[0]}_{available_years[-1]}_{season_to_study}'

                zarr_filename=f'{out_filename}.zarr'

                s3_zarr_name = create_s3_zarr_filename(bucket_name, zarr_filename)

                zarr_file = s3.ls(f'{s3_zarr_name}')

                if len(zarr_file)!=0:
                    print(f'file {s3_zarr_name} already exists. Skipping')
                else:
                    print(f'file {s3_zarr_name} does not exist. Calculating')

                    files_to_download_dask = []
                    local_files_list = []

                    for file_to_download in files:

                        s3_file_to_get = f'{s3_path}{file_to_download}'
                        local_file_location = f'{local_file_path}{file_to_download}'

                        local_files_list.append(local_file_location)

                        if not os.path.isfile(local_file_location):
                            files_to_download_dask.append(dask.delayed(get_file)(s3, s3_file_to_get, local_file_location))

                    out = dask.compute(*files_to_download_dask)    
                    
                    print('completed downloads, now opening as dataset')
                    dataset=xar.open_mfdataset(local_files_list, preprocess=sel_summer)    


                    # nc_filename = f'{out_filename}.nc'

                    newds=dataset

                    print('writing to zarr')

                    if not os.path.isdir(zarr_filename):
                        newds.to_zarr(zarr_filename, consolidated=True)

                    dataset.close()

                    print('uploading to s3')

                    upload_zarr_to_s3(bucket_name, zarr_filename, s3_zarr_name)

                    print('removing local files')
                    for local_file in local_files_list:
                        os.remove(local_file)
                        
                    # os.remove(nc_filename)
                    shutil.rmtree(zarr_filename)

                    