import warnings
warnings.filterwarnings('ignore')
from dask.distributed import Client, LocalCluster
import xarray as xr
import os
import time
from IPython.display import display
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

USER = "x-arnav1710"
xr.set_options(display_style="html")
client = None

def initClient():
    global client
    cluster = LocalCluster(n_workers=16)
    client = Client(cluster)

def shutDownClient():
    global client
    client.close()

def maxVelocity(data, sliceStart, sliceEnd):
    subset_data = data.sel(feature_id = slice(sliceStart, sliceEnd))
    subset_data["velocity"].max().values
    
def maxStreamFlow(data, sliceStart, sliceEnd):
    subset_data = data.sel(feature_id = slice(sliceStart, sliceEnd))
    subset_data["streamflow"].max().values
    
tests = [
    ["Max Velocity Test", maxVelocity],
    ["Max StreamFlow Test", maxStreamFlow]
]

# Design chunking schemas in a way such that querying
# any feature of the point data is quick

schemas = [
    ["Everything100", {"time" : 100, "feature_id" : 100}]
]

# we only care about point data here

# researchers would only want to query
# specific data points out

REPEAT_FOR = 5

def openDataSet(reg = ""):
    dirpath = "/anvil/projects/x-cis220065/x-cybergis/compute/WRFHydro-Example-Output/CHRTOUT/"
    dataset = xr.open_mfdataset(dirpath + "*" + reg + ".CHRTOUT_DOMAIN1", 
                                engine="netcdf4", combine="nested",
                                concat_dim="time", parallel="True")
    return dataset

def chunkForScheme(data, dimension, chunkScheme):
    data[dimension] = data[dimension].chunk(chunkScheme)
    return data

def commitToZarr(data, filePath):
    data.to_zarr(filePath, mode="w")

def getTime(data, test):
    start = time.time()
    for _ in range(REPEAT_FOR):
        test[1](data, 3199274, 10038154)
    end = time.time()
    tot = (end - start) / REPEAT_FOR
    return tot

def chunkAndTest(chunkOn):
    if 'cached_dataset' not in globals():
        globals()['cached_dataset'] = openDataSet("")

    dataset = globals()['cached_dataset'].copy()
    for schema in schemas:
        filePath = f"/anvil/scratch/{USER}/{schema[0]}.zarr"
        if not os.path.exists(filePath):
            dataset = chunkForScheme(dataset, chunkOn, schema[1])
            commitToZarr(dataset, filePath)
    times = []
    for schema in schemas:
        filePath = f"/anvil/scratch/{USER}/{schema[0]}.zarr"
        for test in tests:
            data = xr.open_zarr(filePath)
            times.append(getTime(data, test))
    return times

if __name__ == '__main__':
    initClient()
    test_res = chunkAndTest("velocity")
    test_res_str = chunkAndTest("streamflow")
    print(test_res)
    print(test_res_str)
    shutDownClient()