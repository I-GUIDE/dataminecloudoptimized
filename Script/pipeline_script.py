import matplotlib.pyplot as plt
import os
import time
import numpy as np
import pandas as pd
import xarray as xr
import dask.config
from dask.distributed import Client
from dask_jobqueue import SLURMCluster
from testcases import testcases
from tqdm import tqdm

# Change to your username if running on anvil
USERNAME = "x-michaelade"
NUM_REPETITIONS = 5
NUM_WORKERS = 128
MEMORY = "936 GB"

# dask client
client = None 

def initialize() ->None:
  dask.config.set({"array.slicing.split_large_chunks": False})

  cluster = SLURMCluster(
    queue='highmem',
    cores = 128,
    n_workers=NUM_WORKERS, 
    memory=MEMORY,
    walltime="04:00:00")
  
  cluster.scale(jobs = 1)

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
  openTimes = []

  for idx, scheme in enumerate(tqdm(chunkSchemes)):
    fullpath = os.path.join(f"/anvil/scratch/{USERNAME}/", f"{scheme['name']}.zarr")
    print(f"\tTesting scheme: {scheme['name']}")

    startTime = time.time() # measure the time it takes to open the file
    ds = xr.open_zarr(fullpath)
    openTimes.append(time.time() - startTime)

    times.append(GetTestTime(ds, testcases))


  times = np.array(times)
  openTimes = np.array(openTimes)

  plt.figure(figsize=(12, 6))

  # Save as csv
  openTimesDataFrame = pd.DataFrame(
    data = openTimes, 
    index = [scheme['name'] for scheme in chunkSchemes],
    columns = "open_time");
  
  openTimesDataFrame.to_csv("Output/openTimes.csv")

  # Create a line plot
  plt.plot([scheme['name'] for scheme in chunkSchemes], openTimes)

  # Add labels and title
  plt.xlabel('Chunk Size')
  plt.ylabel(f'Time to open file in seconds')
  plt.title(f'Graph of opening time for chunk scheme')

  # Save the chart to a file (e.g., as a PNG image)
  plt.savefig(f'Output/Images/time_to_open.png')

  # avoid overlap
  plt.close()

  # Save as csv
  openTimesDataFrame = pd.DataFrame(
    data = times, 
    index = [scheme['name'] for scheme in chunkSchemes],
    columns = testcases[:]['name']);
  openTimesDataFrame.to_csv("Output/computeTimes.csv")

  for idx, test in enumerate(testcases):
    plt.figure(figsize=(12, 6))

    # Create a line plot
    plt.plot([scheme['name'] for scheme in chunkSchemes], times[:, idx])

    # Add labels and title
    plt.xlabel('Chunk Size')
    plt.ylabel(f'Time for test case in seconds')
    plt.title(f'{testcases[idx]['name']}')

    # Save the chart to a file (e.g., as a PNG image)
    plt.savefig(f'Output/Images/{testcases[idx]['name']}_{pictureName}.png')

    # avoid overlap
    plt.close()

def Rechunk(chunkSchemes) -> None:
  print("Attempting to re-chunk dataset")
  times = []

  rrt = None
  for idxx, scheme in enumerate(tqdm(chunkSchemes)):
    startTime = time.time() # calculate the time it takes to save

    if not os.path.exists(f"/anvil/scratch/{USERNAME}/{scheme['name']}.zarr"):

      if rrt is None:
        print(f"Missing scheme found ({scheme['name']})! Loading entire dataset...")

        path = "/anvil/datasets/ncar/AORC_Forcing/2016/"
        files = os.listdir(path)[:50]

        dataset = xr.open_mfdataset(
          [os.path.join(path, file) for file in files], 
          parallel=True,
          combine='nested',
          concat_dim='time',
          engine = "netcdf4")
      
        rrt = dataset["RAINRATE"]

      print(f"Re-chunking using scheme {scheme['name']}")

      rrt = rrt.chunk(scheme['scheme'])
      rrt.to_zarr(f"/anvil/scratch/{USERNAME}/{scheme['name']}.zarr", mode="w")

    times.append(time.time() - startTime)

  if rrt is not None:
    OutputTimeToChunkGraph(times)

def OutputTimeToChunkGraph(data):
  plt.figure(figsize=(12, 6))

  # Create a line plot
  plt.plot([scheme['name'] for scheme in chunkSchemes], data)

  # Add labels and title
  plt.xlabel('Chunk Size')
  plt.ylabel(f'Time to rechunk and save in seconds')
  plt.title(f'Graph of rechunking for chunk size')

  # Save the chart to a file (e.g., as a PNG image)
  plt.savefig(f'Output/Images/time_to_rechunk.png')

  # avoid overlap
  plt.close()

if __name__ == "__main__" :
  #Start the client
  initialize()

  # Test chunk schemes across time
  chunkSchemes = [
    {'name': "Day-Med", 'scheme': {"time": 24, "x": 1, "y": 1}},
    {'name': "Week-Med", 'scheme': {"time": 24*7, "x": 1, "y": 1}},
    {'name': "Month-Med", 'scheme': {"time": 24*7*4, "x": 1, "y": 1}},
    {'name': "Month-Large", 'scheme': {"time": 24*7*4, "x": 6, "y": 6}},
    {'name': "6-Months-Large", 'scheme': {"time": 24*7*24, "x": 6, "y": 6}},
    {'name': "Year-Large", 'scheme': {"time": 24*365, "x": 6, "y": 6}},
    ]

  try :
    Rechunk(chunkSchemes)
    RunTestCases("chunk_test", testcases, chunkSchemes)

  # We make sure to always shutdown safely  
  finally:
    shutdown()

