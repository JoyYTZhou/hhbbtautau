source scripts/venv.sh

export PROCESS_NAME=WZ

rm -f testresult.log
python3 src/main.py > testresult.log 2>&1 &
