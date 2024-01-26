import matplotlib.pyplot as plt
import os
import time
import numpy as np
import xarray as xr
from dask.distributed import Client, LocalCluster
from testcases import testcases

# Change to your username if running on anvil
USERNAME = "x-michaelade"
NUM_REPETITIONS = 5
NUM_WORKERS = 64
MEMORY = "512GB"

# dask client
client = None

# TODO
# Measure time to create and save chunks
# Test across the time dimension 

def initialize() ->None:
  cluster = LocalCluster(n_workers=NUM_WORKERS, memory_limit=1/(NUM_WORKERS + 1))
  client = Client(cluster)

def shutdown() ->None:
  client.close()


def GetTestTime(datatset, testcases) -> int:
  times = []

  for func in testcases:
    print(f"\t\tTest case: {func['name']}")
    timetoadd = 0
    startTime = time.time()

    for i in range(NUM_REPETITIONS):
      func['func'](datatset)
      
    endTime = time.time()
    timetoadd += endTime - startTime
    timetoadd = timetoadd / NUM_REPETITIONS

    times.append(timetoadd)
    
  return times

def RunTestCases(pictureName, testcases, chunkSchemes) -> None:
  print("Running benchmark test cases")
  times = []

  for scheme in chunkSchemes:
    fullpath = os.path.join(f"/anvil/scratch/{USERNAME}/", f"{scheme['name']}.zarr")
    print(f"\tTesting scheme: {scheme['name']}")
    ds = xr.open_zarr(fullpath)
    times.append(GetTestTime(ds, testcases))

  times = np.array(times)

  for idx, test in enumerate(testcases):
    plt.figure(figsize=(12, 6))

    # Create a line plot
    plt.plot([scheme['name'] for scheme in chunkSchemes], times[:, idx])

    # Add labels and title
    plt.xlabel('Chunk Size')
    plt.ylabel(f'Time for test case')
    plt.title(f'{testcases[idx]['name']}')

    # Save the chart to a file (e.g., as a PNG image)
    plt.savefig(f'{testcases[idx]['name']}_{pictureName}.png')

    # avoid overlap
    plt.close()

def Rechunk(chunkSchemes) -> None:
  print("Attempting to re-chunk dataset")

  rrt = None
  for scheme in chunkSchemes:
    if not os.path.exists(f"/anvil/scratch/{USERNAME}/{scheme['name']}.zarr"):

      if rrt is None:
        print("Missing scheme found! Loading entire dataset...")
        ds = xr.open_mfdataset("/anvil/projects/x-cis220065/x-cybergis/compute/AORC_Forcing/2016/*.LDASIN_DOMAIN1", engine = "netcdf4", combine= "nested", concat_dim="Time", parallel = "True")
        rrt = ds["RAINRATE"]

      print(f"Re-chunking using scheme {scheme['name']}")

      rrt = rrt.chunk(scheme['scheme'])
      rrt.to_zarr(f"/anvil/scratch/{USERNAME}/{scheme['name']}.zarr", mode="w")
  
if __name__ == "__main__" :
  #Start the client
  initialize()

  # Test chunk schemes across time
  chunkSchemes = [
    {'name': "10x100x100", 'scheme': {"Time": 10, "south_north": 100, "west_east": 100}},
    {'name': "50x300x300", 'scheme': {"Time": 50, "south_north": 300, "west_east": 300}},
    {'name': "100x600x600", 'scheme': {"Time": 100, "south_north": 600, "west_east": 600}},
    {'name': "150x900x900", 'scheme': {"Time": 150, "south_north": 900, "west_east": 900}},
    {'name': "200x1200x1200", 'scheme': {"Time": 200, "south_north": 1200, "west_east": 1200}},
    ]

  Rechunk(chunkSchemes)
  RunTestCases("chunk_test", testcases, chunkSchemes)

  shutdown()

