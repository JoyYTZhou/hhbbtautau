# ==============================================================================
# Updated on: June 3, 2024
# Used to: create dynamic job submissions for different datasets
# ==============================================================================

DISABLE_SUBMISSION=false

while getopts ":d" opt; do
  case ${opt} in
    d )
      DISABLE_SUBMISSION=true
      ;;
    \? )
      echo "Invalid option: -$OPTARG" 1>&2
      exit 1
      ;;
    : )
      echo "Invalid option: -$OPTARG requires an argument" 1>&2
      exit 1
      ;;
  esac
done
shift $((OPTIND -1))

DYNACONF_ENV=$1
PROCESS_INPUT=$2

cd ..
source scripts/venv.sh $DYNACONF_ENV
cd exec

JOB_DIRNAME=$(python3 -c 'from src.analysis.spawndask import rs; print(rs.JOB_DIRNAME)')
rm -rf ${JOB_DIRNAME}/*.json
python3 genjobs.py

if [ "$PROCESS_INPUT" = "ALL" ]; then
    declare -a PROCESS_NAMES=("QCD" "DYJets" "TTbar" "WW" "WWW" "ZH" "ZZ" "WZ" "ggF" "SingleH")
else
    declare -a PROCESS_NAMES=("$PROCESS_INPUT")
fi

for PROCESS in "${PROCESS_NAMES[@]}"; do
    export PROCESS_NAME=$PROCESS


    \cp -f hhbbtt.sub runtime/${JOB_DIRNAME}_${PROCESS}.sub

    cat << EOF >> runtime/${JOB_DIRNAME}_${PROCESS}.sub
JOB_DIRNAME = ${JOB_DIRNAME}
PROCESS_NAME = ${PROCESS}
DYNACONF = ${ENV_FOR_DYNACONF}
queue FILENAME matching files ${JOB_DIRNAME}/${PROCESS}_*.json
EOF

    if [ "$DISABLE_SUBMISSION" = false ]; then
        condor_submit runtime/${JOB_DIRNAME}_${PROCESS}.sub
    else
        echo "Submission disabled for process: $PROCESS"
    fi

done

