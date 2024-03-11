export PYTHONPATH=$PWD/src:$PYTHONPATH
export ENV_FOR_DYNACONF=LPCJUPYTER

python3 src/plot.py > plot.log 2>&1 &