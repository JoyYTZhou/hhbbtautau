source scripts/venv.sh

export PROCESS_NAME=DYJets

rm -f testresult.log
python3 src/main.py --diagnose > testresult.log 2>&1 &
