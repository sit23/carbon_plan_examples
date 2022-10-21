import xarray as xar

base_dir = '/Users/stephenthomson/Downloads/'

files = [f'{base_dir}air.2m.gauss.{year}.nc' for year in range(2003,2007)]

dataset = xar.open_mfdataset(files)


summer_air_temp = dataset['air'].where(dataset['time'].dt.season=='JJA', drop=True)

latitude_of_location = 50.
longitude_of_location = 0.

summer_temp_at_location = summer_air_temp.sel(lat=latitude_of_location, lon=longitude_of_location, method='nearest')

summer_temp_at_location.to_dataframe().to_csv('test.csv')