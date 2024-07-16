# ==============================================================================
# Updated on: June 3, 2024
# Used to: create dynamic job submissions for different datasets
# ==============================================================================

export PROCESS_NAME=$1

source ../scripts/envutil.sh
setup_dirname_local
LPC_setup

cd ..
source scripts/venv.sh
cd exec

PROCESS_DATA=$DATA_DIR/preprocessed
XRD_DIRECTOR=root://cmsxrootd.fnal.gov
FILENAME=${PROCESS_DATA}/${PROCESS_NAME}.json
INDX=$(jq 'length' $FILENAME)
LEN=$((INDX-1))

python3 skimjob.py

cp -f hhbbtt.sub runtime/hhbbtt_${PROCESS_NAME}.sub

cat << EOF >> runtime/hhbbtt_${PROCESS_NAME}.sub
PROCESS_NAME = ${PROCESS_NAME}
queue FILENAME matching files skimjson/${PROCESS_NAME}_*.json
EOF
