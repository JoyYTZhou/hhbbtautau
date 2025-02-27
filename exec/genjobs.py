from src.analysis.spawnjobs import JobLoader, pjoin
from config.projectconfg import runsetting as rs
import os
import sys

cwd = os.getcwd()
projectbase = os.path.dirname(cwd)

def gen_jobs(groupname):
    arg_1 = int(sys.argv[1]) if sys.argv[1] else 5
    jl = JobLoader(datapath=pjoin(projectbase, 'data', rs.JOB_PATH), kwd=groupname, jobpath=pjoin(cwd, rs.JOB_DIRNAME),
                   transferPBase=rs.TRANSFER_PATH, out_endpattern=rs.get('OUTENDPATTERN', [".root", "cutflow.csv"]))
    jl.writejobs(batch_size=arg_1)

if __name__ == '__main__':
    gen_jobs(sys.argv[1])
