#!/bin/sh -l

if [ $# -eq 0 ]
  then
  echo "Pass in the name of your desired anaconda environment"
  exit
fi

module load anaconda/2021.05-py38
conda activate $1

echo "Started at: $(date)"
python pipeline_script.py
echo "Finished at : $(date)"