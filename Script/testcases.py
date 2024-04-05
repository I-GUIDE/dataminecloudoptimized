import xarray as xr
import dask.dataframe as dr

# Test Cases
def Bench_MaxRain(datatset: xr.Dataset):
  values = datatset['RAINRATE'].max().compute().values

# Day ===============================================
def Average_Small_Day(datatset: xr.Dataset):
  area = datatset.sel(x=0, y=0, time=slice(0, 24))
  values = area['RAINRATE'].mean(dim="time").compute().values

def Average_Med_Day(datatset: xr.Dataset):
  area = datatset.sel(x=slice(40.21, 40.71), y=slice(-111.7, -110.9), time=slice(0, 24))
  values = area['RAINRATE'].mean(dim="time").compute().values

def Average_Large_Day(datatset: xr.Dataset):
  area = datatset.sel(x=slice(36.048020187527, 41.8308079035245), y=slice(36.048020187527, 41.8308079035245), time=slice(0, 24))
  values = area['RAINRATE'].mean(dim="time").compute().values

# Week ===============================================
def Average_Small_Week(datatset: xr.Dataset):
  area = datatset.sel(x=0, y=0, time=slice(0, 24*7))
  values = area['RAINRATE'].mean(dim="time").compute().values

def Average_Med_Week(datatset: xr.Dataset):
  area = datatset.sel(x=slice(40.21, 40.71), y=slice(-111.7, -110.9), time=slice(0, 24*7))
  values = area['RAINRATE'].mean(dim="time").compute().values

def Average_Large_Week(datatset: xr.Dataset):
  area = datatset.sel(x=slice(36.048020187527, 41.8308079035245), y=slice(36.048020187527, 41.8308079035245), time=slice(0, 24*7))
  values = area['RAINRATE'].mean(dim="time").compute().values

# Month ===============================================
def Average_Small_Month(datatset: xr.Dataset):
  area = datatset.sel(x=0, y=0, time=slice(0, 24*7*4))
  values = area['RAINRATE'].mean(dim="time").compute().values

def Average_Med_Month(datatset: xr.Dataset):
  area = datatset.sel(x=slice(40.21, 40.71), y=slice(-111.7, -110.9), time=slice(0, 24*7*4))
  values = area['RAINRATE'].mean(dim="time").compute().values

def Average_Large_Month(datatset: xr.Dataset):
  area = datatset.sel(x=slice(36.048020187527, 41.8308079035245), y=slice(36.048020187527, 41.8308079035245), time=slice(0, 24*7*4))
  values = area['RAINRATE'].mean(dim="time").compute().values

# 6 Month ===============================================
def Average_Small_6Month(datatset: xr.Dataset):
  area = datatset.sel(x=0, y=0, time=slice(0, 24*7*4*6))
  values = area['RAINRATE'].mean(dim="time").compute().values

def Average_Med_6Month(datatset: xr.Dataset):
  area = datatset.sel(x=slice(40.21, 40.71), y=slice(-111.7, -110.9), time=slice(0, 24*7*4*6))
  values = area['RAINRATE'].mean(dim="time").compute().values

def Average_Large_6Month(datatset: xr.Dataset):
  area = datatset.sel(x=slice(36.048020187527, 41.8308079035245), y=slice(36.048020187527, 41.8308079035245), time=slice(0, 24*7*4*6))
  values = area['RAINRATE'].mean(dim="time").compute().values

# Year ===============================================
def Average_Small_Year(datatset: xr.Dataset):
  area = datatset.sel(x=0, y=0, time=slice(0, 24*7*4))
  values = area['RAINRATE'].mean(dim="time").compute().values

def Average_Med_Year(datatset: xr.Dataset):
  area = datatset.sel(x=slice(40.21, 40.71), y=slice(-111.7, -110.9), time=slice(0, 24*7*4*6))
  values = area['RAINRATE'].mean(dim="time").compute().values

def Average_Large_Year(datatset: xr.Dataset):
  area = datatset.sel(x=slice(36.048020187527, 41.8308079035245), y=slice(36.048020187527, 41.8308079035245), time=slice(0, 24*7*4*6))
  values = area['RAINRATE'].mean(dim="time").compute().values


# Used for the test cases
testcases = [
  {'name': 'Max_Rain_Test', 'func': Bench_MaxRain},

  {'name': 'Average_Small_Day', 'func': Average_Small_Day},
  {'name': 'Average_Med_Day', 'func': Average_Med_Day},
  {'name': 'Average_Large_Day', 'func': Average_Large_Day},

  {'name': 'Average_Small_Week', 'func': Average_Small_Week},
  {'name': 'Average_Med_Week', 'func': Average_Med_Week},
  {'name': 'Average_Large_Week', 'func': Average_Large_Week},

  {'name': 'Average_Small_Month', 'func': Average_Small_Month},
  {'name': 'Average_Med_Month', 'func': Average_Med_Month},
  {'name': 'Average_Large_Month', 'func': Average_Large_Month},
  
  {'name': 'Average_Small_6Month', 'func': Average_Small_6Month},
  {'name': 'Average_Med_6Month', 'func': Average_Med_6Month},
  {'name': 'Average_Large_6Month', 'func': Average_Large_6Month},
  
  {'name': 'Average_Small_Year', 'func': Average_Small_Year},
  {'name': 'Average_Med_Year', 'func': Average_Med_Year},
  {'name': 'Average_Large_Year', 'func': Average_Large_Year},
  ]
