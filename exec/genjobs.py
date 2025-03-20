from src.analysis.spawnjobs import JobLoader, pjoin
from config.projectconfg import runsetting as rs
import argparse
import os

cwd = os.getcwd()
projectbase = os.path.dirname(cwd)

def gen_jobs(groupname, batch_size, babyjob_dirname):
    jl = JobLoader(datapath=pjoin(projectbase, 'data', rs.JOB_DIR), kwd=groupname, jobpath=pjoin(cwd, babyjob_dirname),
                   transferPBase=rs.TRANSFER_PATH, out_endpattern=rs.get('OUTENDPATTERN', [".root", "cutflow.csv"]))
    jl.writejobs(batch_size=batch_size)

if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description='Generate job files for processing')
    argparser.add_argument('groupname', type=str, help='Name of the group to generate jobs for')
    argparser.add_argument('babyjobpath', type=str, help='Name of the directory to store the baby job files')
    argparser.add_argument('--batch', type=int, default=5, help='Number of files per job')
    args = argparser.parse_args()
    gen_jobs(args.groupname, args.batch, args.babyjobpath)