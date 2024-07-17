source scripts/venv.sh

export PROCESS_NAME=QCD

rm -f testresult.log
python3 src/main.py --input 'exec/skimjson/QCD_0_job_0.json' --diagnose > testresult.log 2>&1 &
