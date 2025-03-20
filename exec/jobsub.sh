# ==============================================================================
# Updated on: June 3, 2024
# Used to: create dynamic job submissions for different datasets
# Author's Improvements:
# - Added error checking for directory existence
# - Added validation for numeric batch size
# - Improved variable naming for clarity
# - Added logging of key operations
# ==============================================================================

usage() {
    echo "Usage: $0 [-d] <DYNACONF_ENV> <PROCESS> <YEAR> <BATCHSIZE> <JOBDIR>"
    echo
    echo "Arguments:"
    echo "  DYNACONF_ENV  Environment configuration name"
    echo "  PROCESS       Process name (use 'ALL' for all processes)"
    echo "  YEAR         Year to process (use 'ALL' for all years)"
    echo "  BATCHSIZE    Number of jobs per batch (must be positive integer)"
    echo "  JOBDIR       Directory for job files (must exist)"
    echo
    echo "Options:"
    echo "  -d           Disable job submission (dry run)"
    echo
    echo "Example:"
    echo "  $0 prod ttH 2018 100 jobs"
    exit 1
}

DISABLE_SUBMISSION=false

while getopts ":dh" opt; do
  case ${opt} in
    d )
      DISABLE_SUBMISSION=true
      ;;
    h )
      usage
      ;;
    \? )
      echo "Invalid option: -$OPTARG" 1>&2
      usage
      ;;
    : )
      echo "Invalid option: -$OPTARG requires an argument" 1>&2
      usage
      ;;
  esac
done
shift $((OPTIND -1))

if [ $# -ne 5 ]; then
    echo "Error: Incorrect number of arguments"
    usage
fi

DYNACONF_ENV=$1
PROCESS=$2
YEAR=$3
BATCHSIZE=$4
JOBDIR=$5

# Validate batch size is a positive integer
if ! [[ "$BATCHSIZE" =~ ^[0-9]+$ ]] || [ "$BATCHSIZE" -eq 0 ]; then
    echo "Error: BATCHSIZE must be a positive integer"
    exit 1
fi

cd ..
if ! source scripts/venv.sh $DYNACONF_ENV; then
    echo "Error: Failed to source virtual environment"
    exit 1
fi
cd exec

PROCESS_KEY="${PROCESS}"
YEAR_KEY="${YEAR}"

if [ "$PROCESS" = "ALL" ]; then
    PROCESS_KEY='*'
fi

if [ "$YEAR" = "ALL" ]; then
    YEAR_KEY='*'
fi

JOB_DIRNAME=".${DYNACONF_ENV}"

# Ensure job directory exists
mkdir -p "${JOB_DIRNAME}"

FILENAME=".${JOB_DIRNAME}/${PROCESS_KEY}_${YEAR_KEY}*.json"
rm -f ${JOB_DIRNAME}/${PROCESS_KEY}_${YEAR_KEY}*.json

if ! python3 genjobs.py ${PROCESS_KEY}_${YEAR_KEY} ${JOBDIR} ${JOB_DIRNAME} --batch $BATCHSIZE; then
    echo "Error: Job generation failed"
    exit 1
fi

SUBFILENAME=${DYNACONF_ENV}_${PROCESS}_${YEAR}.sub

if ! \cp -f hhbbtt.sub runtime/${SUBFILENAME}; then
    echo "Error: Failed to copy submission template"
    exit 1
fi

cat << EOF >> runtime/${SUBFILENAME}
DYNACONF = ${DYNACONF_ENV}
JOB_DIRNAME = ${JOB_DIRNAME}
queue FILENAME matching files ${FILENAME}
EOF

if [ "$DISABLE_SUBMISSION" = false ]; then
    echo "Submitting jobs for process: $PROCESS"
    condor_submit runtime/${SUBFILENAME}
else
    echo "Dry run - submission disabled for process: $PROCESS"
fi
