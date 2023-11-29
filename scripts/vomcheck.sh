#!/bin/bash

# Check if VOMS proxy exists
if voms-proxy-info --exists; then
  echo "VOMS proxy exists."

  # Check if the VOMS proxy is valid
  if voms-proxy-info --valid 10 2>&1 > /dev/null; then
    echo "VOMS proxy is valid."
    # Optionally, you can display time left for the proxy
    timeleft=$(voms-proxy-info --timeleft)
    echo "Time left for the proxy: ${timeleft} seconds."
  else
    echo "VOMS proxy is not valid or has expired."
  fi
else
  echo "No VOMS proxy found."
fi
