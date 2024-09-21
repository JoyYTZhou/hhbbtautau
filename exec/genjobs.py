from src.analysis.spawnjobs import JobLoader, pjoin
from config.projectconfg import runsetting as rs
import os

cwd = os.getcwd()
projectbase = os.path.dirname(cwd)

def gen_jobs():
    jl = JobLoader(datapath=pjoin(projectbase, 'data', 'preprocessed'), jobpath=pjoin(cwd, rs.JOB_DIRNAME),
                   transferPBase=rs.TRANSFER_PATH)
    jl.writejobs()

if __name__ == '__main__':
    gen_jobs()
