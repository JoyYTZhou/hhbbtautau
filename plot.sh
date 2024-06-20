source scripts/venv.sh
export ENV_FOR_DYNACONF=LPCTEST
rm -f plot.log

# python3 src/plot.py --postprocess > plot.log 2>&1 &
# python3 src/plot.py --getcf > plot.log 2>&1 &
python3 src/plot.py --plotouts > plot.log 2>&1 &
# python3 src/plot.py --checkouts > plot.log 2>&1 &

