source scripts/venv.sh

export PYTHONPATH=$PWD/src:$PYTHONPATH
export PROCESS_NAME=ggF
export DEBUG_MODE=true
export ENV_FOR_DYNACONF=PRESELECT

python3 src/main.py > testresult.log 2>&1 &
