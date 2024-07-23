from analysis.spawndask import JobLoader, pjoin, rs
import os, shutil

cwd = os.path.dirname(os.path.abspath(__file__))

def gen_jobs():
    folder = pjoin(cwd, rs.JOB_DIRNAME)
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))
    jl = JobLoader(pjoin(cwd, rs.JOB_DIRNAME))
    jl.writejobs()

if __name__ == '__main__':
    gen_jobs()
