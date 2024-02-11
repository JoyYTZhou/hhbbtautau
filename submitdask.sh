source lpcsetup.sh
source scripts/venv.sh
export IS_CONDOR=true
export PROCESS_NAME=$1 
export SUBMIT_DASK=true

python3 src/main.py
