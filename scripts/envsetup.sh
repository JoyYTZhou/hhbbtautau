#!/bin/bash

cd ~/work/hhbbtautau
rm -r *.tar.gz
source scripts/sasetup.sh
cd ~/nobackup
export ENV_NAME=newcoffea

python -m venv ${ENV_NAME}
echo "creating new venv..."
source ${ENV_NAME}/bin/activate

python3 -m pip install coffea
echo "Installed Coffea"
python3 -m pip install dynaconf --no-cache-dir
echo "Installed dyanconf"
python3 -m pip install vector --upgrade --no-cache-dir
echo "Installed vector"
python3 -m pip install dask --no-cache-dir
echo "Installed dask"
python3 -m pip install dask-jobqueue --no-cache-dir
echo "Installed dask-jobqueue"
python3 -m pip install dask-awkward --no-cache-dir
echo "Installed dask-awkward"
python3 -m pip install dask-histogram --no-cache-dir
echo "Installed dask-histogram"
python3 -m pip install htcondor --no-cache-dir
echo "Installed htcondor"
deactivate

# create tarball
tar -czf ~/work/hhbbtautau/${ENV_NAME}.tar.gz ${ENV_NAME}
cd ~/work/hhbbtautau
