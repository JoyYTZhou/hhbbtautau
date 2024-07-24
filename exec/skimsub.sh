# ==============================================================================
# Updated on: June 3, 2024
# Used to: create dynamic job submissions for different datasets
# ==============================================================================

PROCESS_INPUT=$1
DYNACONF_ENV=$2

cd ..
source scripts/venv.sh $DYNACONF_ENV
cd exec

if [ "$PROCESS_INPUT" = "ALL" ]; then
    declare -a PROCESS_NAMES=("QCD" "DYJets" "TTbar" "WW" "WWW" "ZH" "ZZ" "WZ" "ggF" "SingleH")
else
    declare -a PROCESS_NAMES=("$PROCESS_INPUT")
fi

for PROCESS in "${PROCESS_NAMES[@]}"; do
    PROCESS_DATA=$DATA_DIR/preprocessed
    FILENAME=${PROCESS_DATA}/${PROCESS}.json
    INDX=$(jq 'length' $FILENAME)
    LEN=$((INDX-1))
    export PROCESS_NAME=$PROCESS

    JOB_DIRNAME=$(python3 -c 'from analysis.spawndask import rs; print(rs.JOB_DIRNAME)')

    python3 genjobs.py

    cp -f hhbbtt.sub runtime/${JOB_DIRNAME}_${PROCESS}.sub

    cat << EOF >> runtime/${JOB_DIRNAME}_${PROCESS}.sub
JOB_DIRNAME = ${JOB_DIRNAME}
PROCESS_NAME = ${PROCESS}
DYNACONF = ${ENV_FOR_DYNACONF}
queue FILENAME matching files ${JOB_DIRNAME}/${PROCESS}_*.json
EOF

condor_submit runtime/${JOB_DIRNAME}_${PROCESS}.sub
done

