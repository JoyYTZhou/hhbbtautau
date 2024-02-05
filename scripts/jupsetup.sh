#!/usr/bin/bash

source /cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos7-gcc11-opt/setup.sh

export HHBBTT=$PWD
export PYTHONPATH=$HHBBTT/src:$PYTHONPATH

jupyter notebook --no-browser --port=2001


