source scripts/lpcsetup.sh
source scripts/venv.sh

export PYTHONPATH=$PWD/src:$PYTHONPATH

export PROCESS_NAME=ggF

python3 src/main.py > testresult.log 2>&1
