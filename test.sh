source scripts/lpcsetup.sh
source scripts/sasetup.sh
source ~/nobackup/newcoffea/bin/activate

export PYTHONPATH=$PWD/src:$PYTHONPATH
export ENV_FOR_DYNACONF=LPCLOCAL

python3 src/main.py > dask.log 2>&1