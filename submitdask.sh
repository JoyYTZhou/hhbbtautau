export ENV_FOR_DYNACONF=LPCCONDOR

python3 src/main.py > dask.log 2>&1 &
