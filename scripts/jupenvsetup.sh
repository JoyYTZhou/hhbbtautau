#!/usr/bin/bash
source /cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos7-gcc11-opt/setup.sh
cd ~/nobackup
export ENV_NAME=coffeajup
python -m venv ${ENV_NAME}
echo "creating new venv..."
source ${ENV_NAME}/bin/activate

python3 -m pip install coffea --upgrade --no-cache-dir
echo "Installed Coffea"
python3 -m pip install dynaconf --no-cache-dir
echo "Installed dyanconf"
python3 -m pip install vector --upgrade --no-cache-dir
echo "Installed vector"
deactivate


