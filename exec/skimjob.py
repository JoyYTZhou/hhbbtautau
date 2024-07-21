from analysis.spawndask import JobLoader, pjoin
import os, argparse

cwd = os.path.dirname(os.path.abspath(__file__))

def gen_jobs():
    jl = JobLoader(pjoin(cwd, 'skimjson'))
    jl.writejobs()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    gen_jobs()
