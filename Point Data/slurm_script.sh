#!/bin/bash

sbatch -t 4:00:00 -p highmem --job-name 'Benchmark Point Data Zarr' --nodes=1 --ntasks=64 job.sh I-GUIDE