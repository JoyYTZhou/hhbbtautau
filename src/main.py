# UPDATE TIME: 2023-09-15
# FROM JOY
import glob
from config.selectionconfig import runsetting as rs
from analysis.selutility import *

if rs.TEST_MODE:
    proc = Processor(rs)
    proc.setdata()
    filename, partitions = list(proc.data.items())[0]
    # print("processing filename, partitions: ", filename, partitions)
    passed, cf = proc.singlerun(f"{filename}:Events", suffix="")
    print("computing...")
    cf_np, cf_lab = proc.res_to_np(cf)
    cf_df = pd.DataFrame(data=cf_np, columns=proc.channelseq, index=cf_lab)
    print("writing to csv...")
    cf_df.to_csv(pjoin(proc.outdir, "cutflow.csv"))
else:
    pass
