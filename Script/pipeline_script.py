import matplotlib.pyplot as plt
import os
import time
import dask.config
import numpy as np
import pandas as pd
import xarray as xr
from dask.distributed import Client, LocalCluster
from dask_jobqueue import SLURMCluster
from testcases import testcases, GetTestTime
from tqdm import tqdm
from chunk import Rechunk

# Change to your username if running on anvil
USERNAME = "x-michaelade"
NUM_WORKERS = 64
MEMORY = "1031 GB"

def initialize():
  dask.config.set({'logging.distributed': 'error'})

  global cluster
  cluster = SLURMCluster(
    queue='highmem',
    cores = 128,
    n_workers=NUM_WORKERS, 
    memory=MEMORY,
    account="cis220065",
    walltime='04:00:00')
  cluster.scale(jobs = 2)

  global client
  client = Client(cluster)

  os.system('clear')

def shutdown():
  client.close()
  cluster.close()

def RunTestCases(pictureName, testcases, chunkSchemes) -> None:
  print("\n\tRunning benchmark")
  chunkNames = [scheme['name'] for scheme in chunkSchemes]
  times = []
  openTimes = []

  for scheme in (bar := tqdm(chunkSchemes, ascii=' ░▒█')):
    bar.set_description(scheme['name'])

    fullpath = os.path.join(f"/anvil/scratch/{USERNAME}/", f"{scheme['name']}.zarr")
    startTime = time.time() # measure the time it takes to open the file
    ds = xr.open_zarr(fullpath)
    openTimes.append(time.time() - startTime)

    times.append(GetTestTime(ds, testcases))
    bar.desc = "Completed benchmark"


  times = np.array(times)
  openTimes = np.array(openTimes)

  plt.figure(figsize=(12, 6))

  # Save as csv
  openTimesDataFrame = pd.DataFrame(
    data = openTimes, 
    index = chunkNames,
    columns = ["open_time"]);

  openTimesDataFrame.to_csv("Output/openTimes.csv")

  # Create a line plot
  plt.bar(chunkNames, openTimes)

  # Add labels and title
  plt.xlabel('Chunk Size')
  plt.ylabel(f'Time to open file in seconds')
  plt.title(f'Graph of opening time for chunk scheme')

  # Save the chart to a file (e.g., as a PNG image)
  plt.savefig(f'Output/Images/time_to_open.png')

  # avoid overlap
  plt.close()


  # Save as csv
  testcasesDataFrame = pd.DataFrame(
    data = times, 
    index = chunkNames,
    columns = [test['name'] for test in testcases]);
  testcasesDataFrame.to_csv("Output/computeTimes.csv")

  for idx, test in enumerate(testcases):
    plt.figure(figsize=(12, 6))

    # Create a line plot
    plt.bar(chunkNames, times[:, idx])

    # Add labels and title
    plt.xlabel('Chunk Size')
    plt.ylabel(f'Time for test case in seconds')
    plt.title(f'{test['name']}')

    # Save the chart to a file (e.g., as a PNG image)
    plt.savefig(f'Output/Images/{test['name']}_{pictureName}.png')

    # avoid overlap
    plt.close()

if __name__ == "__main__" :
  initialize()

  # Test chunk schemes across time
  chunkSchemes = [
    {'name': "Day-Med", 'scheme': {"time": 24, "x": 80, "y": 70}},
    {'name': "Week-Med", 'scheme': {"time": 24*7, "x": 80, "y": 70}},
    {'name': "Month-Med", 'scheme': {"time": 24*7*4, "x": 80, "y": 70}},
    {'name': "Day-Small", 'scheme': {"time": 24, "x": 8, "y": 7}},
    {'name': "Week-Small", 'scheme': {"time": 24*7, "x": 8, "y": 7}},
    {'name': "Month-Small", 'scheme': {"time": 24*7*4, "x": 8, "y": 7}},
    ]
  
  Rechunk(chunkSchemes, USERNAME)

  #Start the cluster

  RunTestCases("chunk_test", testcases, chunkSchemes)

  shutdown()

