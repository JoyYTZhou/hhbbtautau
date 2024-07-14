from analysis.spawndask import JobLoader, pjoin
import os

cwd = os.path.dirname(os.path.abspath(__file__))

def gen_jobs():
    jl = JobLoader(pjoin(cwd, 'skimjson'))
    jl.writejobs()

if __name__ == '__main__':
    gen_jobs()
