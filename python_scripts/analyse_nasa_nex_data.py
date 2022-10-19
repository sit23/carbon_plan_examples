import xarray as xar
import s3fs
import pdb

def create_s3_zarr_filename(bucket_name, local_file_to_upload):

    zarr_name = local_file_to_upload.split('/')[-1]
    outname = f's3://{bucket_name}/nasa_nex_downscaling/{zarr_name}'

    return outname

def generate_local_zarr_name(variable_to_search, experiment_to_search, model_to_search, available_years, season_to_study):

    out_filename = f'{variable_to_search}_{experiment_to_search}_{model_to_search}_{available_years[0]}_{available_years[-1]}_{season_to_study}'

    zarr_filename=f'{out_filename}.zarr'

    return zarr_filename


if __name__=="__main__":



    bucket_name='reanalysis-data'

    season_to_study='JJA'

    latitude_of_location = 50.7260
    longitude_of_location = 360.-3.5275

    for model_to_search in ['EC-Earth3']:
        for variable_to_search in ['tasmax', 'hurs']:
            for experiment_to_search in ['historical', 'ssp585']:

                if experiment_to_search=='historical':
                    # 1950-1999
                    available_years = [f'{num}' for num in range(1950,2000)]
                    # available_years = [f'{num}' for num in range(1950,1955)]                
                else:
                    #end of century
                    available_years = [f'{num}' for num in range(2080,2099)]


                zarr_filename = generate_local_zarr_name(variable_to_search, experiment_to_search, model_to_search, available_years, season_to_study)

                full_output_path = create_s3_zarr_filename(bucket_name, zarr_filename)

                s3=s3fs.S3FileSystem(anon=True,    client_kwargs={
                'region_name': 'eu-west-2'
                }) #initialise the filesystem, including the anon=True option that can be used for public buckets
                store = s3fs.S3Map(root=full_output_path, s3=s3, check=False) #sets up the map to the object

                zarr = xar.open_zarr(store=store) #opens the dataset as a zarr object straight from s3

                #Actually subset the data based on location and ensemble member
                ds_at_location = zarr[variable_to_search].sel(lat=latitude_of_location, lon=longitude_of_location, method='nearest')


                dset_name=f'NASA_NEX_GDDP_CMIP6.{model_to_search}.{experiment_to_search}.day.NASA_NEX'

                #Specify the name of my output file
                filename_out = f'{dset_name}_{latitude_of_location:.2f}_{longitude_of_location:.2f}_{season_to_study}.csv'

                #Send it to a csv file
                ds_at_location.to_dataframe().to_csv(filename_out)