import xarray as xr

# Test Cases
def Bench_MaxRain(datatset: xr.Dataset):
  datatset['RAINRATE'].max().values

def Bench_Average_200sqr(datatset: xr.Dataset):
  area = datatset.sel(south_north=slice(200, 400), west_east=slice(200,400))
  area['RAINRATE'].mean(dim="Time").values

def Bench_Average_400sqr(datatset: xr.Dataset):
  area = datatset.sel(south_north=slice(000, 400), west_east=slice(000,400))
  area['RAINRATE'].mean(dim="Time").values

def Bench_Average_800sqr(datatset: xr.Dataset):
  area = datatset.sel(south_north=slice(000, 800), west_east=slice(000,800))
  area['RAINRATE'].mean(dim="Time").values

def Bench_Average_1600sqr(datatset: xr.Dataset):
  area = datatset.sel(south_north=slice(000, 1600), west_east=slice(000,1600))
  area['RAINRATE'].mean(dim="Time").values


# Used for the test cases
testcases = [
  {'name': 'Max_Rain_Test', 'func': Bench_MaxRain},
  {'name': 'Average_200_sqr', 'func': Bench_Average_200sqr},
  {'name': 'Average_400_sqr', 'func': Bench_Average_400sqr},
  {'name': 'Average_800_sqr', 'func': Bench_Average_800sqr},
  {'name': 'Average_1600_sqr', 'func': Bench_Average_1600sqr},
  ]
