#!/bin/bash

cd ~/work/hhbbtautau
rm -r *.tar.gz
source scripts/sasetup.sh
cd ~/nobackup
python -m venv newcoffea
echo "creating new venv..."
source newcoffea/bin/activate
export ENV_NAME=newcoffea
python3 -m pip install coffea
echo "Installed Coffea"
python3 -m pip install dynaconf --no-cache-dir
echo "Installed dyanconf"
python3 -m pip install vector --upgrade --no-cache-dir
echo "Installed vector"
deactivate

# create tarball
tar -czf ~/work/hhbbtautau/${ENV_NAME}.tar.gz ${ENV_NAME}
cd ~/work/hhbbtautau
