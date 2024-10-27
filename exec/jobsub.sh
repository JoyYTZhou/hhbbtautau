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
PRCESS=$2
YEAR=$3

cd ..
source scripts/venv.sh $DYNACONF_ENV
cd exec

JOB_DIRNAME=$(python3 -c 'from config.projectconfg import runsetting as rs; print(rs.JOB_DIRNAME)')

if [ "$PROCESS" = "ALL" ]; then
  PROCESS_KEY='*'
else 
  PROCESS_KEY=$PROCESS
fi

if [ "$YEAR" = "ALL" ]; then
  YEAR_KEY='*'
else
  YEAR_KEY=$YEAR
fi

FILENAME="${JOB_DIRNAME}/${PROCESS_KEY}_${YEAR_KEY}.json"
rm -rf ${JOB_DIRNAME}/${PROCESS_KEY}_${YEAR_KEY}.json
python3 genjobs.py ${PROCESS_KEY}_${YEAR_KEY}

SUBFILENAME=${DYNACONF_ENV}_${PROCESS}_${YEAR}.sub

\cp -f hhbbtt.sub runtime/${SUBFILENAME}

cat << EOF >> runtime/${SUBFILENAME}
DYNACONF = ${DYNACONF_ENV}
JOB_DIRNAME = ${JOB_DIRNAME}
queue FILENAME matching files ${FILENAME}
EOF

if [ "$DISABLE_SUBMISSION" = false ]; then
    condor_submit runtime/${SUBFILENAME}
else
    echo "Submission disabled for process: $PROCESS"
fi


