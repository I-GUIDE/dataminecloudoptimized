#!/bin/bash

sbatch -A cis220065 -t 4:00:00 -p highmem --job-name 'Benchmark Zarr Chunks' --nodes=1 --ntasks=64 job.sh I-GUIDE