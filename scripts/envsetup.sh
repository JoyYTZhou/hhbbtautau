#!/bin/bash
# ===========================================================================================================
# This script sets up a proper virtual environment to run this repository and create a tarball
# in the ~/nobackup area
# ===========================================================================================================

# NOTE: This script is meant for LCG set up with PYTHON<=3.10
source scripts/envutil.sh
setup_dirname_local
LCG_sasetup

cd $BASE_DIR
rm -r *.tar.gz
cd ~/nobackup

echo "=================================================================="
echo "Please enter the name of the new virtual environment: "
read ENV_NAME
echo "=================================================================="

rm ${ENV_NAME}.tar.gz
rm -r ${ENV_NAME}

python -m venv ${ENV_NAME} --system-site-packages
echo "creating new venv..."
source ${ENV_NAME}/bin/activate

pip install --upgrade pip
python -m pip install coffea --upgrade --no-cache-dir
echo "Installed coffea"
python -m pip install dynaconf --no-cache-dir
echo "Installed dyanconf"
python -m pip install htcondor --no-cache-dir
echo "Installed htcondor"
python -m pip install rucio-clients==32 --no-cache-dir
echo "Installed ruico-clients"
python -m pip install hist --upgrade --no-cache-dir
echo "Installed hist"
python -m pip install uproot --upgrade --no-cache-dir
echo "Installed uproot"
python -m pip install matplotlib --upgrade --no-cache-dir
echo "Installed matplotlib"
python -m pip install dask --upgrade --no-cache-dir
echo "Installed dask"
python -m pip install dask_awkward --upgrade --no-cache-dir
python -m pip install pympler --upgrade --no-cache-dir
python -m pip install memory-profiler --upgrade --no-cache-dir
python -m pip install line-profiler --upgrade --no-cache-dir
python -m pip install "dask[distributed]" --upgrade --no-cache-dir
python -m pip install msgpack --upgrade --no-cache-dir

deactivate

# create tarball
tar -czf ${BASE_DIR}/${ENV_NAME}.tar.gz ${ENV_NAME}
cd ${BASE_DIR}
