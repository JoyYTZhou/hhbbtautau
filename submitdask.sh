# =================================================================
# To launch jobs from local node
# =================================================================

export ENV_FOR_DYNACONF=LPCLOCAL

python3 src/main.py > dask_${ENV_FOR_DYNACONF}.log 2>&1 &
