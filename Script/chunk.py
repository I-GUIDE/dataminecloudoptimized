import matplotlib.pyplot as plt
import os
import time
import pandas as pd
import xarray as xr
from tqdm import tqdm
from dask.distributed import wait

def Rechunk(chunkSchemes, username) -> None:
    print("Attempting to re-chunk dataset")
    times = []

    rrt = None

    for scheme in (bar := tqdm(chunkSchemes, ascii=" ░▒█")):
        bar.set_description(scheme["name"])
        startTime = time.time()  # calculate the time it takes to save

        if not os.path.exists(f"/anvil/scratch/{username}/{scheme['name']}.zarr"):
            if rrt is None:
                bar.set_description("Loading dataset...")

                path = "/anvil/datasets/ncar/AORC_Forcing/2013/"
                files = os.listdir(path)[:1000]

                dataset = xr.open_mfdataset(
                    [os.path.join(path, file) for file in files],
                    parallel=True,
                    combine="nested",
                    concat_dim="time",
                    engine="netcdf4",
                )

                rrt = dataset["RAINRATE"]
                bar.set_description(scheme["name"])
                
            startTime = time.time()
            rrt = rrt.chunk(scheme["scheme"])
            rrt.to_zarr(f"/anvil/scratch/{username}/{scheme['name']}.zarr", mode="w")

        times.append(time.time() - startTime)
        bar.desc = "Finished"

    if rrt is not None:
        OutputTimeToChunkGraph(times, chunkSchemes)

        # Save as csv
        chunkTimesGraph = pd.DataFrame(
            data=times,
            index=[scheme["name"] for scheme in chunkSchemes],
            columns=["time_to_chunk"],
        )
        chunkTimesGraph.to_csv("Output/timeToChunk.csv")


def OutputTimeToChunkGraph(data, chunkSchemes):
    plt.figure(figsize=(12, 6))

    # Create a line plot
    plt.plot([scheme["name"] for scheme in chunkSchemes], data)

    # Add labels and title
    plt.xlabel("Chunk Size")
    plt.ylabel(f"Time to rechunk and save in seconds")
    plt.title(f"Graph of rechunking for chunk size")

    # Save the chart to a file (e.g., as a PNG image)
    plt.savefig(f"Output/Images/time_to_rechunk.png")

    # avoid overlap
    plt.close()
