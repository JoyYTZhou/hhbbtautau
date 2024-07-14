# ==============================================================================
# Updated on: June 3, 2024
# Used to: create dynamic job submissions for different datasets
# ==============================================================================

PROCESS_NAME=$1

source ../scripts/envutil.sh
setup_dirname_local
LPC_setup

PROCESS_DATA=$DATA_DIR/preprocessed
XRD_DIRECTOR=root://cmsxrootd.fnal.gov

FILENAME=${PROCESS_DATA}/${PROCESS_NAME}.json
INDX=$(jq 'length' $FILENAME)
LEN=$((INDX-1))

python3 skimjob.py

cp -f hhbbtt.sub runtime/hhbbtt_${PROCESS_NAME}.sub
cat << EOF >> runtime/hhbbtt_${PROCESS_NAME}.sub
PROCESS_NAME = ${PROCESS_NAME}
queue arg2 from seq 0 ${LEN} |
EOF
