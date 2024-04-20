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
                cpusno=8
                memory=16GB
            else
                echo "File size is greater than 1GB. Submitting in loops."
                copyfile=true
                if [[ $size -lt 20000000000 ]]; then
                    cpusno=10
                    memory=22GB
                else
                    cpusno=12
                    memory=24GB
                fi
            fi 
        fi
    done 
    if $continue_flag; then
        continue
    else 
        echo "Creating new run job script for ${file}"
        cp skim.sh runtime/skim_${file}.sh

        cat << EOF >> runtime/skim_${file}.sh
        export PROCESS_NAME=${file}
        export COPY_FILE=${copyfile}
        python3 src/main.py
EOF
        echo "Creating new submission job script for ${file}"
        cp skim.sub runtime/skim_${file}.sub
        cat << EOF >> runtime/skim_${file}.sub
        request_cpus = ${cpusno}
        request_memory = ${memory}
        request_disk = 5GB
        queue
EOF
    fi
  done
done

# json_files=($(for f in "$DATA_DIR/preprocessed"/*.json; do basename "$f" .json; done))
# for name in "${json_files[@]}"; do
#     echo "Reading $name.json"
# done

