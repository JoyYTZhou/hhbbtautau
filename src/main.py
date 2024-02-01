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
    passed, cf = proc.singlerun({filename: partitions}, suffix="_test")
    print("computing...")
    cf_np, cf_lab = proc.res_to_np(cf)
    cf_df = pd.DataFrame(data=cf_np, columns=proc.channelseq, index=cf_lab)
    print("writing to csv...")
    cf_df.to_csv(pjoin(proc.outdir, "cutflow.csv"))
else:
    proc.runall()


