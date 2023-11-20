# Get the current PATH into an array
IFS=':' read -r -a array <<< "$PATH"

# Use associative array to remove duplicates
declare -A seen
result=()

for i in "${array[@]}"; do
  if [[ ! -v seen[$i] ]]; then
      result+=("$i")
      seen["$i"]=1
    fi
done

# Join the result array back into a string
new_path=$(IFS=":"; echo "${result[*]}")

# Set the new PATH
export PATH="$new_path"

# Get the current PYTHONPATH into an array
IFS=':' read -r -a array <<< "$PYTHONPATH"

# Use associative array to remove duplicates
declare -A seen
result=()

for i in "${array[@]}"; do
  if [[ ! -v seen[$i] ]]; then
      result+=("$i")
      seen["$i"]=1
    fi
done

# Join the result array back into a string
new_pythonpath=$(IFS=":"; echo "${result[*]}")

# Set the new PYTHONPATH
export PYTHONPATH="$new_pythonpath"


