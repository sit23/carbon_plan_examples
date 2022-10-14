import intake
import matplotlib.pyplot as plt

#open the Carbon Plan datastore as a catalogue, so that I can subset it for the data I need:
cat = intake.open_esm_datastore(
    "https://cpdataeuwest.blob.core.windows.net/cp-cmip/version1/catalogs/global-downscaled-cmip6.json"
)

#Look at a subset 
cat_subset = cat.search(
    method="GARD-MV",
    source_id="MRI-ESM2-0",
    experiment_id="ssp245",
    variable_id="tasmax",
)

dsets = cat_subset.to_dataset_dict()

print(dsets)

ds = dsets["ScenarioMIP.MRI.MRI-ESM2-0.ssp245.day.GARD-MV"]

latitude_of_location = 50.7260
longitude_of_location = -3.5275

member_to_use = ds['member_id'].values[0]

ds_at_location = ds['tasmax'].sel(lat=latitude_of_location, lon=longitude_of_location, member_id=member_to_use, method='nearest')

end_of_century = ds_at_location.sel(time=slice('2080-01-01', '2099-12-31'))

end_of_century_summer = end_of_century.where(end_of_century.time.dt.season=='JJA', drop=True)

end_of_century_summer.to_dataframe().to_csv('exeter.csv')