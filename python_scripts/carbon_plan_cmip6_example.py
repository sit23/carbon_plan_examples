import intake
import matplotlib.pyplot as plt
import numpy as np
import pdb

#open the Carbon Plan datastore as a catalogue, so that I can subset it for the data I need:
cat = intake.open_esm_datastore(
    "https://cpdataeuwest.blob.core.windows.net/cp-cmip/version1/catalogs/global-downscaled-cmip6.json"
)

#In the next section I'm looking at what data is available in the entire catalogue:

cat_df = cat.df #First convert the catalogue to a pandas dataframe

#Look for unique entries in each of the columns of the dataframe

models_avail = np.unique([key for key in cat_df['source_id']])

methods_avail = np.unique([key for key in cat_df['method']])

exps_avail = np.unique([key for key in cat_df['experiment_id']])

vars_avail = np.unique([key for key in cat_df['variable_id']])

#Loop over each of the models available
for model_to_check in models_avail:

    #Search the catalogue for all the data related to that specific model
    cat_subset = cat.search(
        source_id = model_to_check,
    )

    #Turn it into a pandas dataframe again
    cat_subset_df = cat_subset.df

    #Summarise the data available for that model:

    methods_avail_subs = np.unique([key for key in cat_subset_df['method']])
    exps_avail_subs = np.unique([key for key in cat_subset_df['experiment_id']])
    vars_avail_subs = np.unique([key for key in cat_subset_df['variable_id']])

    print(f'For the {model_to_check} climate model, the following data is available:') 
    print(f'The downscaling methods available are {methods_avail_subs}')
    print(f'The experiments that are available are {exps_avail_subs}')
    print(f'The downscaled variables that are available are {vars_avail_subs}')
    print(f'\n')


#Now that we know what's available, let's choose a specific model, specific downscaling method, a specific experiment and a specfic variable to look at:

model_to_search = 'MRI-ESM2-0'
method_to_search = 'GARD-SV'
experiment_to_search = 'historical'
variable_to_search = 'tasmax'

#Let's now specifiy the location of the place I want to study:

latitude_of_location = 50.7260
longitude_of_location = -3.5275
season_to_study = 'JJA'

#Search the catalogue for my search criteria
cat_search_subset = cat.search(
    source_id=model_to_search, #This is the name of the climate model used    
    method=method_to_search, #This is the downscaling method used
    experiment_id=experiment_to_search, #This is the name of the experiment, e.g. ssp245
    variable_id=variable_to_search, #This is the name of the variable I want to look at, e.g. tasmax, which is the maximum surface air temperature.
)

#Turn my catalogue search into xarray datasets
dsets = cat_search_subset.to_dataset_dict()

#Perform a check to see if I have any results that match my search.
if len(dsets)==0:
    raise NotImplementedError('The catalogue search that you specified has returned zero results. One reason for this could be that you have mis-spelled a variable name somewhere, or that the data does not exist in the archive')
else:
    print(f'You have found {len(dsets)} datasets that match your search criteria. These are:')
    for dset_name in dsets.keys():
        print(f'{dset_name} \n')

#Loop over each dataset in my list to find a timeseries at that location, subset it for summer, and write it out to a csv file.
for dset_name in dsets.keys():

    ds = dsets[dset_name] #open the relevant dataset

    exp_id_of_ds = ds.attrs['experiment_id'] #find out which experiement it is

    member_to_use = ds['member_id'].values[0] #Always look at the first ensemble member

    #Actually subset the data based on location and ensemble member
    ds_at_location = ds[variable_to_search].sel(lat=latitude_of_location, lon=longitude_of_location, method='nearest').sel(member_id=member_to_use)


    #Specify the time periods I want to look at
    if exp_id_of_ds=='historical':
        #If using historical data then just look at whole thing
        time_slice = ds_at_location 
    else:
        #If using a climate change scenario then look at the end of the century        
        time_slice = ds_at_location.sel(time=slice('2080-01-01', '2099-12-31'))

    #Select only the summer months (JJA in the northern hemisphere)
    time_slice_summer = time_slice.where(time_slice.time.dt.season==season_to_study, drop=True)

    #Specify the name of my output file
    filename_out = f'{dset_name}_{latitude_of_location:.2f}_{longitude_of_location:.2f}_{season_to_study}.csv'

    #Send it to a csv file
    time_slice_summer.to_dataframe().to_csv(filename_out)