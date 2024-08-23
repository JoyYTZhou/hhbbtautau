from src.analysis.spawndask import JobLoader, pjoin, rs
<<<<<<< HEAD
import os
=======
import os, shutil
>>>>>>> f931ec4 (refactor python path)

cwd = os.path.dirname(os.path.abspath(__file__))

def gen_jobs():
    jl = JobLoader(pjoin(cwd, rs.JOB_DIRNAME))
    jl.writejobs()

if __name__ == '__main__':
    gen_jobs()
