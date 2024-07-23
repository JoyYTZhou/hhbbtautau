# ==============================================================================
# Updated on: June 3, 2024
# Used to: create dynamic job submissions for different datasets
# ==============================================================================

export PROCESS_NAME=$1
DYNACONF_ENV=$2

cd ..
source scripts/venv.sh $DYNACONF_ENV
cd exec

PROCESS_DATA=$DATA_DIR/preprocessed
XRD_DIRECTOR=root://cmsxrootd.fnal.gov
FILENAME=${PROCESS_DATA}/${PROCESS_NAME}.json
INDX=$(jq 'length' $FILENAME)
LEN=$((INDX-1))

JOB_DIRNAME=$(python3 -c 'from analysis.spawndask import rs; print(rs.JOB_DIRNAME)')

python3 genjobs.py

cp -f hhbbtt.sub runtime/${JOB_DIRNAME}_${PROCESS_NAME}.sub

cat << EOF >> runtime/${JOB_DIRNAME}_${PROCESS_NAME}.sub
JOB_DIRNAME = ${JOB_DIRNAME}
PROCESS_NAME = ${PROCESS_NAME}
DYNACONF = ${ENV_FOR_DYNACONF}
queue FILENAME matching files ${JOB_DIRNAME}/${PROCESS_NAME}_*.json
EOF

condor_submit runtime/${JOB_DIRNAME}_${PROCESS_NAME}.sub
