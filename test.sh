source scripts/venv.sh

export PROCESS_NAME=ggF
export DEBUG_MODE=false

rm -f testresult.log
python3 src/main.py > testresult.log 2>&1 &
