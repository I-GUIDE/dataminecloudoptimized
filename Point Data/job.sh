#!/bin/bash

source bench/bin/activate

echo "Started at: $(date)"
python3 point_data_script.py
echo "Finished at : $(date)"