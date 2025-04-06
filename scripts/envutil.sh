#!/bin/bash
# ===========================================================================================================
# This script contains some functions to set up LPC environment for analysis quickly 
# Last updated: 2024-09-21
# ===========================================================================================================

function setup_dirname_local {
    NAME=$(whoami)
    export BASE_DIR=$(pwd)
    export LIB_DIR=${BASE_DIR}/scripts
    export SRC_DIR=${BASE_DIR}/src
    export DATA_DIR=${BASE_DIR}/data
    export CONDOR_BASE=/store/user/${NAME}
    export OUTPUT_BASE=/uscms/home/${NAME}/nobackup
    echo "Successfully set up environment or $PWD"
}

function human_readable() {
    size=$1
    units=("B" "KB" "MB" "GB" "TB" "PB" "EB" "ZB" "YB")
    for ((i=0; size>=1024 && i<${#units[@]}-1; i++)); do
        size=$(bc <<< "scale=2; $size/1024")
    done
    echo "$size${units[$i]}"
}

function LCG_setup {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        release_version=$VERSION_ID
    else
        echo "Cannot determine OS version"
        echo "Sourcing default centos 7 LCG setup"
        source /cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos7-gcc11-opt/setup.sh
        return 1
    fi

    if [[ "$release_version" == 8.* ]]; then
        echo "Performing operations for release 8.*"
        source /cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos8-gcc11-opt/setup.sh
    elif [[ "$release_version" == 9.* ]]; then
        echo "Performing operations for release 9.*"
        # do not change this, this will have impact on venv installment
        source /cvmfs/sft.cern.ch/lcg/views/LCG_105/x86_64-el9-gcc13-opt/setup.sh 
    else
        source /cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos7-gcc11-opt/setup.sh
    fi
}

function LPC_setup {
    export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
    export EOS_MGM_URL=root://cmseos.fnal.gov
    source $VO_CMS_SW_DIR/cmsset_default.sh

    print_env_variable() { var="$1"; [ -z "${!var}" ] && echo "$var is not set" || echo "$var has been set to ${!var}"; }

    export PREFIX=root://cmseos.fnal.gov
    print_env_variable "PREFIX"

    alias condor_rm_held="condor_rm -constraint 'JobStatus == 5'"
    alias condor_rm_running="condor_rm -constraint 'JobStatus == 2'"
}

function LCG_sasetup {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        release_version=$VERSION_ID
    else
        echo "Cannot determine OS version"
        echo "Sourcing default centos 7 LCG setup"
        version=x86_64-centos7-gcc11-opt
    fi

    if [[ "$release_version" == 8.* ]]; then
        version=x86_64-centos7-gcc11-opt
    elif [[ "$release_version" == 9.* ]]; then
        version=x86_64-el9-gcc13-opt
    else
        version=x86_64-centos7-gcc11-opt
    fi

    echo $version

    source /cvmfs/sft.cern.ch/lcg/releases/LCG_105/Python/3.9.12/$version/Python-env.sh
    echo "Successfully sourced python package"
    source /cvmfs/sft.cern.ch/lcg/releases/LCG_105/ROOT/6.30.02/$version/ROOT-env.sh
    echo "Successfully sourced ROOT software"
    source /cvmfs/sft.cern.ch/lcg/releases/LCG_105/pyyaml/6.0.1/$version/PyYAML-env.sh
    echo "Successfully sourced PyYAML software"
    source /cvmfs/sft.cern.ch/lcg/releases/LCG_105/gdb/14.1/$version/gdb-env.sh
    echo "Successfully sourced gdb software"
}

function set_python_path {
    DEFAULT_PYTHON_HOME=$(python -c "import sys; print(sys.base_prefix)")
    VENV_SITE_PACKAGES=$(python -c "import site; print(site.getsitepackages()[0])")
    export PYTHONHOME=$DEFAULT_PYTHON_HOME
    export PYTHONPATH=$VENV_SITE_PACKAGES:$PYTHONPATH
}

function remove_duplicates {
    IFS=':' read -r -a array <<< "$1"
    declare -A seen
    result=()
    for i in "${array[@]}"; do
        if [[ ! -v seen[$i] ]]; then
            result+=("$i")
            seen["$i"]=1
        fi
    done
    new_path=$(IFS=":"; echo "${result[*]}")
    echo $new_path
}

function checkproxy {
    if [ -z "$X509_USER_PROXY" ]; then
        echo "Proxy not found. Please run `vominit` to create a proxy."
        exit 1
    else
        echo "Proxy found at $X509_USER_PROXY"
        export X509_CERT_DIR=/cvmfs/grid.cern.ch/etc/grid-security/certificates/
        voms-proxy-info -path -debug
        voms-proxy-info -file $X509_USER_PROXY -debug
        return 0
    fi
}

function vominit {
    voms-proxy-init --rfc --voms cms -valid 192:00
}

function checkvom {
    if voms-proxy-info --exists; then
    echo "VOMS proxy exists."

    if voms-proxy-info --valid 10 2>&1 > /dev/null; then
        echo "VOMS proxy is valid."
        timeleft=$(voms-proxy-info --timeleft)
        echo "Time left for the proxy: ${timeleft} seconds."
    else
        echo "VOMS proxy is not valid or has expired."
    fi
    else
    echo "No VOMS proxy found."
    fi
}

function cplocal {
    DIRNAME=$1
    echo "Copying from condor to local ........................"
    xrdcp -r root://cmseos.fnal.gov/${CONDOR_BASE}/${DIRNAME} ${OUTPUT_BASE}
}

function csvview {
    if [ $# -eq 0 ]; then
        echo "Usage: csvview <file.csv> [--title TITLE] [--max-rows N]"
        echo "Example: csvview data.csv --title 'My Data' --max-rows 10"
        return 1
    fi

    if [ -z "$SRC_DIR" ]; then
        echo "Error: SRC_DIR environment variable not set. Please run setup_dirname_local first."
        return 1
    fi

    # Call the displayutil.py directly
    python -m src.utils.displayutil "$@"
}

function sum_genweight {
    if [ $# -eq 0 ]; then
        echo "Usage: sum_genweight <root_file> [tree_name]"
        echo "Example: sum_genweight myfile.root Events"
        return 1
    fi

    ROOT_FILE=$1
    TREE_NAME=${2:-Events}  # Default to 'Events' if not specified

    # Check if file exists and is accessible
    # if [ ! -f "$ROOT_FILE" ]; then
        # echo "Error: File $ROOT_FILE does not exist or is not accessible"
        #return 1
    # fi

    # Execute the Python script directly
    python -m src.utils.rootutil "$ROOT_FILE" "$TREE_NAME"
}

function hadd_and_collect {
    if [ $# -eq 0 ]; then
        echo "Usage: hadd_and_collect <SEL_NAME> <PROCESS_NAME> <YEAR>"
        echo "Example: hadd_and_collect TIGHTSKIM TTbar ALL"
        return 1
    fi

    SEL_NAME=$1
    PROCESS_NAME=$2
    YEAR=$3

    echo -n "Which directory do you want to hadd? (e.g. vbfskim, tightskim, onelooseb, etc.): "
    read DIRNAME

    echo -n "Where do you want to put the job files? (e.g. tightskimmed, vbfskimmed, etc.): "
    read JOB_DIRNAME

    HADDED_DIRNAME=/store/user/joyzhou/${DIRNAME}_hadded

    if [ "$PROCESS" = "Data" ]; then
        python postprocess.py --dirname $DIRNAME --group $PROCESS_NAME --year $YEAR --mode hadd
        FIRST_EXIT_CODE=$?
    else
        python postprocess.py --dirname $DIRNAME --group $PROCESS_NAME --year $YEAR -m --mode hadd
        FIRST_EXIT_CODE=$?
    fi

    # Check if first program executed successfully
    if [ $FIRST_EXIT_CODE -eq 0 ]; then
        echo "Checking corrupted files completed successfully. Starting data collecting..."

        cd data
        # Execute second Python program
        if [ "$PROCESS" = "Data" ]; then
            python datacollect.py -d $PROCESS_NAME -y $YEAR -q $HADDED_DIRNAME -o $JOB_DIRNAME
        else
            python datacollect.py -d $PROCESS_NAME -y $YEAR -q $HADDED_DIRNAME -o $JOB_DIRNAME --is_mc
        fi
    
    SECOND_EXIT_CODE=$?

        if [ $SECOND_EXIT_CODE -eq 0 ]; then
            echo "Both programs completed successfully."
        else
            echo "collecting data failed with exit code $SECOND_EXIT_CODE"
            return $SECOND_EXIT_CODE
        fi
    else
        echo "hadding files failed with exit code $FIRST_EXIT_CODE"
        return $FIRST_EXIT_CODE
    fi
}

function check_and_submit {
    if [ $# -eq 0 ]; then
        echo "Usage: check_and_submit <SEL_NAME> <PROCESS_NAME> <YEAR> <SAMPLE_SIZE>"
        echo "Example: check_and_submit SKIM TTbar ALL 50"
        return 1
    fi

    SEL_NAME=$1
    PROCESS_NAME=$2
    YEAR=$3
    SAMPLE_SIZE=$4

    echo -n "Which directory do you want to check? (e.g. vbfskim, tightskim, onelooseb, etc.): "
    read DIRNAME

    # Execute first Python program and wait for it to complete
    if [ "$PROCESS" = "Data" ]; then
        python postprocess.py --dirname $DIRNAME --group $PROCESS_NAME --year $YEAR --mode check -q
        FIRST_EXIT_CODE=$?
    else
        python postprocess.py --dirname $DIRNAME --group $PROCESS_NAME --year $YEAR -m --mode check -q
        FIRST_EXIT_CODE=$?
    fi

    # Check if first program executed successfully
    if [ $FIRST_EXIT_CODE -eq 0 ]; then
        echo "Checking corrupted files completed successfully. Starting second program..."
    
        # Execute second Python program
        if [ "$PROCESS" = "Data" ]; then
            python postprocess.py --dirname $DIRNAME --group $PROCESS_NAME --year $YEAR --mode clean
        else
            python postprocess.py --dirname $DIRNAME --group $PROCESS_NAME --year $YEAR -m --mode clean
        fi

        SECOND_EXIT_CODE=$?

        if [ $SECOND_EXIT_CODE -eq 0 ]; then
            echo "Both programs completed successfully."
        else
            echo "cleanning failed with exit code $SECOND_EXIT_CODE"
            return $SECOND_EXIT_CODE
        fi
    else
        echo "Checking corrupted files failed with exit code $FIRST_EXIT_CODE"
        return $FIRST_EXIT_CODE
    fi

    cd exec
    ./jobsub.sh $SEL_NAME $PROCESS_NAME $YEAR $SAMPLE_SIZE
}
