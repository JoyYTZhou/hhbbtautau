from src.analysis.spawnjobs import JobLoader, pjoin
from config.projectconfg import runsetting as rs
import os
import sys

cwd = os.getcwd()
projectbase = os.path.dirname(cwd)

def gen_jobs(groupname):
    jl = JobLoader(datapath=pjoin(projectbase, 'data', rs.JOB_PATH), groupname=groupname, jobpath=pjoin(cwd, rs.JOB_DIRNAME),
                   transferPBase=rs.TRANSFER_PATH, out_endpattern=rs.get('OUTENDPATTERN', [".root", "cutflow.csv"]))
    jl.writejobs()

if __name__ == '__main__':
    gen_jobs(sys.argv[1])
