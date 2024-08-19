#!/bin/bash
# ===========================================================================================================
# This script sets up a proper virtual environment to run this repository and create a tarball
# in the ~/nobackup area
# ===========================================================================================================

cd ~/work/hhbbtautau
rm -r *.tar.gz
cd ~/nobackup

echo "=================================================================="
echo "Please enter the name of the new virtual environment: "
read ENV_NAME
echo "=================================================================="

python3 -m venv ${ENV_NAME}
echo "creating new venv..."
source ${ENV_NAME}/bin/activate

python3 -m pip install dynaconf --no-cache-dir
echo "Installed dyanconf"
python3 -m pip install htcondor --no-cache-dir
echo "Installed htcondor"
deactivate

# create tarball
tar -czf ~/work/hhbbtautau/${ENV_NAME}.tar.gz ${ENV_NAME}
cd ~/work/hhbbtautau
