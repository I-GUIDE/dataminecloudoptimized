#!/bin/bash

sbatch -t 4:00:00 -p shared --job-name 'Benchmark Zarr Chunks' --nodes=1 --ntasks=64 job.sh I-GUIDE