import xarray as xar
import s3fs
import pdb
import os

def create_s3_zarr_filename(bucket_name, local_file_to_upload):

    zarr_name = local_file_to_upload.split('/')[-1]
    outname = f's3://{bucket_name}/nasa_nex_downscaling/{zarr_name}'

    return outname

def generate_local_zarr_name(variable_to_search, experiment_to_search, model_to_search, available_years, season_to_study):

    out_filename = f'{variable_to_search}_{experiment_to_search}_{model_to_search}_{available_years[0]}_{available_years[-1]}_{season_to_study}'

    zarr_filename=f'{out_filename}_final.zarr'

    return zarr_filename


if __name__=="__main__":



    bucket_name='reanalysis-data'

    season_to_study='JJA'

    latitude_of_location = 40.7128
    longitude_of_location = 360.-74.006

    for model_to_search in ['EC-Earth3']:
        print(model_to_search)
        for variable_to_search in ['tasmax', 'hurs']:
            print(variable_to_search)
            for experiment_to_search in ['historical', 'ssp585']:
                print(experiment_to_search)

                if experiment_to_search=='historical':
                    # 1950-1999
                    available_years = [f'{num}' for num in range(1950,2000)]
                    # available_years = [f'{num}' for num in range(1950,1955)]                
                else:
                    #end of century
                    available_years = [f'{num}' for num in range(2080,2099)]


                dset_name=f'NASA_NEX_GDDP_CMIP6.{model_to_search}.{experiment_to_search}.day.NASA_NEX'

                #Specify the name of my output file
                filename_out = f'{variable_to_search}_{dset_name}_{latitude_of_location:.2f}_{longitude_of_location:.2f}_{season_to_study}.csv'

                if not os.path.isfile(filename_out):
                    zarr_filename = generate_local_zarr_name(variable_to_search, experiment_to_search, model_to_search, available_years, season_to_study)

                    full_output_path = create_s3_zarr_filename(bucket_name, zarr_filename)

                    s3=s3fs.S3FileSystem(anon=True,    client_kwargs={
                    'region_name': 'eu-west-2'
                    }) #initialise the filesystem, including the anon=True option that can be used for public buckets
                    store = s3fs.S3Map(root=full_output_path, s3=s3, check=False) #sets up the map to the object

                    print('opening zarr')
                    zarr = xar.open_zarr(store=store, consolidated=True) #opens the dataset as a zarr object straight from s3

                    print(f"max longitude is {zarr['lon'].max().values}")

                    if longitude_of_location<0. and zarr['lon'].max().values > 180.:
                        raise NotImplementedError('CHECK YOUR LONGITUDE CONVENTION!!')

                    #Actually subset the data based on location and ensemble member
                    ds_at_location = zarr[variable_to_search].sel(lat=latitude_of_location, lon=longitude_of_location, method='nearest')

                    print('writing to csv file')
                    #Send it to a csv file
                    ds_at_location.to_dataframe().to_csv(filename_out)