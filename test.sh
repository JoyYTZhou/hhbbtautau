source scripts/venv.sh

export PROCESS_NAME=WWZ

rm -f testresult.log
python3 src/main.py --input 'exec/skimjson/WWZ_0_job_0.json' --diagnose > testresult.log 2>&1 &
