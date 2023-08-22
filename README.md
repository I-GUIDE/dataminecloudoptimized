# DataMine 2023 Project: Managing Massive Weather Data in the Cloud

## Project Summary
Hydrologic modeling both requires the use of high-resolution retrospective micro-meteorological data and produces high-resolution outputs comprising 10s of variables for the area of interest. 
For long-term simulations (5 years or more) of the National Water Model’s WRF-Hydro, these datasets can quickly run into the terabytes. These raw datasets are in the NetCDF format which presents 
challenges for extracting specific variables and subsetting to the region and period of interest. This project will evaluate various cloud-optimized data formats such as Zarr or Parquet, 
benchmarking various chunking/subsetting operations, and identifying the best format to support these operations on a variety of retrospective datasets. The transformed (cloud-optimized) data would 
then be stored in OSN (Open Storage Network) for public access via popular Python packages such as XArray or Dask.

