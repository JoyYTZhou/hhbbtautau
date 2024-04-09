#!/bin/bash
# ===========================================================================================================
# This script contains some functions to set up LPC environment for analysis quickly 
# ===========================================================================================================

function setup_LCG {
    output=$(lsb_release -a)
    release_version=$(echo "$output" | grep 'Release:' | awk '{print $2}')

    if [[ "$release_version" == 8.* ]]; then
        echo "Performing operations for release 8.*"
        source /cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos8-gcc11-opt/setup.sh
    elif [[ "$release_version" == 9.* ]]; then
        echo "Performing operations for release 9.*"
        source /cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-el9-gcc13-opt/setup.sh
    else
        source /cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos7-gcc11-opt/setup.sh
    fi
}

function LCG_sasetup {
    output=$(lsb_release -a)
    release_version=$(echo "$output" | grep 'Release:' | awk '{print $2}')
    echo $release_version

    if [[ "$release_version" == 8.* ]]; then
        version=x86_64-centos8-gcc11-opt
    elif [[ "$release_version" == 9.* ]]; then
        version=x86_64-el9-gcc13-opt
    else
        version=x86_64-centos7-gcc11-opt
    fi

    echo $version

    source /cvmfs/sft.cern.ch/lcg/releases/LCG_104swan/Python/3.9.12/$version/Python-env.sh
    echo "Successfully sourced python package"
    source /cvmfs/sft.cern.ch/lcg/releases/LCG_104swan/ROOT/6.28.04/$version/ROOT-env.sh
    echo "Successfully sourced ROOT software"
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
