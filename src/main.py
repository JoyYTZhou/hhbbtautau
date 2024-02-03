# UPDATE TIME: 2024-02-01
# FROM JOY
import glob
from config.selectionconfig import runsetting as rs
from analysis.selutility import *

proc = Processor(rs)

if rs.TEST_MODE:
    proc.setdata()
    filename, partitions = list(proc.data.items())[0]
    # print("processing filename, partitions: ", filename, partitions)
    # passed, cf = proc.singlerun({filename: partitions}, suffix="_test")
    passed, cf = proc.singlerun({filename: "Events"}, suffix="_test")
    print("computing...")
    cf_df = proc.res_to_df(cf)
    print("writing to csv...")
    cf_df.to_csv(pjoin(proc.outdir, "cutflow.csv"))
else:
    proc.runmultiple()


