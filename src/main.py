# UPDATE TIME: 2024-02-01
# FROM JOY
import glob
from config.selectionconfig import runsetting as rs
from analysis.selutility import *
import os

proc = Processor(rs)

if rs.TEST_MODE:
    proc.runmultiple(0,9)
else:
    proc.runmultiple()
