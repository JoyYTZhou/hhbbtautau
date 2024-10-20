from src.analysis.spawnjobs import JobLoader, pjoin
from config.projectconfg import runsetting as rs
import os

cwd = os.getcwd()
projectbase = os.path.dirname(cwd)

def gen_jobs():
    jl = JobLoader(datapath=pjoin(projectbase, 'data', rs.JOB_PATH), jobpath=pjoin(cwd, rs.JOB_DIRNAME),
                   transferPBase=rs.TRANSFER_PATH, out_endpattern=rs.get('OUTENDPATTERN', [".root", "cutflow.csv"]))
    jl.writejobs()

if __name__ == '__main__':
    gen_jobs()
