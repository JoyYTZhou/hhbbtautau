#!/bin/bash
# ===========================================================================================================
# This script is used to submit condor jobs
# ===========================================================================================================

source ../scripts/envutil.sh
setup_dirname_local
source ${LIB_DIR}/lpcsetup.sh

echo "Name condor output directory"
read condoroutput

echo "full condor path will be ${CONDOR_BASE}/${condoroutput}"

xrdfs $PREFIX stat ${CONDOR_BASE}/${condoroutput}
if $? -eq 0; then
    echo "Directory already exists. This could be a resume job."
    read -p "Do you want to continue? (y/n)" -n 1 -r
fi

PROCESS_DATA=$DATA_DIR/preprocessed
XRD_DIRECTOR=root://cmsxrootd.fnal.gov
for file in "${PROCESS_DATA}"/*.json; do
    echo "Reading ${file}"
    keys=$(jq -r 'keys[]' "$file")
    continue_flag=false
    for key in $keys; do
        echo "Dataset name: $key"
        filelist=($(jq -r ".$key.filelist[]" "$file"))
        random_index=$((RANDOM % ${#filelist[@]}))
        random_file=${filelist[$random_index]}
        random_file=${random_file#"${XRD_DIRECTOR}/"}
        echo "Querying file with index {$random_index}..."
        output=$(xrdfs $XRD_DIRECTOR stat $random_file)
        if [ $? -ne 0 ]; then
            echo "XRD query failed for file ${random_file}... skipping. Not submitting condor jobs for {$file}"
            continue_flag=true
        else 
            size=$(echo "$output" | grep -oP 'Size:\s+\K\d+')
            if [[ $size -lt 1000000000 ]]; then
                echo "File size is less than 1GB. Initiating future jobs."
                copyfile=false
            else
                echo "File size is greater than 1GB. Submitting in loops."
                copyfile=true
            fi 
        fi
    done 
    if $continue_flag; then
        continue
    else 
        echo "Writing to skim.sh ..."
        cp skim.sh runtime/skim_${file}.sh

        cat << EOF >> runtime/skim_${file}.sh
        export PROCESS_NAME=${file}
        export COPY_FILE=${copyfile}
EOF
        cp skim.sub runtime/skim_${file}.sub
        echo "Submitting condor jobs."
    fi
  done
    echo "Submitting ${file}"
    python ${LIB_DIR}/submit.py -c ${file} -o ${CONDOR_BASE}/${condoroutput}
done

json_files=($(for f in "$DATA_DIR/preprocessed"/*.json; do basename "$f" .json; done))
for name in "${json_files[@]}"; do
    echo "Reading $name.json"
done

