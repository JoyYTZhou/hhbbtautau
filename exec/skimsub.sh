# ==============================================================================
# Updated on: June 3, 2024
# Used to: create dynamic job submissions for different datasets
# ==============================================================================

export PROCESS_NAME=$1
export DYNACONF_ENV=$2

cd ..
source scripts/venv.sh $DYNACONF_ENV
cd exec

PROCESS_DATA=$DATA_DIR/preprocessed
XRD_DIRECTOR=root://cmsxrootd.fnal.gov
FILENAME=${PROCESS_DATA}/${PROCESS_NAME}.json
INDX=$(jq 'length' $FILENAME)
LEN=$((INDX-1))

python3 genjobs.py

cp -f hhbbtt.sub runtime/hhbbtt_${PROCESS_NAME}.sub

cat << EOF >> runtime/hhbbtt_${PROCESS_NAME}.sub
PROCESS_NAME = ${PROCESS_NAME}
DYNACONF = ${ENV_FOR_DYNACONF}
queue FILENAME matching files skimjson/${PROCESS_NAME}_*.json
EOF
