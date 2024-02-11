# UPDATE TIME: 2024-02-01
# FROM JOY
import os
import time
from dask.distributed import Client, LocalCluster
from dask_jobqueue.htcondor import HTCondorCluster
import yaml
import dask.config

parent_dir = os.path.dirname(__file__)
pjoin = os.path.join
files_needed = f"""{pjoin(parent_dir, 'src')}, 
{pjoin(parent_dir, 'lpcsetup.sh')},
{pjoin(parent_dir, 'setup.sh')},
{pjoin(parent_dir, 'scripts')},
{pjoin(parent_dir, 'newcoffea.tar.gz')},
{pjoin(parent_dir, 'dirsetup.sh')}
"""

process = os.getenv('PROCESS_NAME', 'all')

def spawnclient():
    if not os.getenv("IS_CONDOR", False): 
        cluster = LocalCluster(processes=False, threads_per_worker=2)
        cluster.adapt(minimum=0, maximum=6)
        client = Client(cluster)
        print("successfully created a dask client!")
        print("===================================")
        print(client)
    else:
        if os.getenv("SUBMIT_DASK", False):
            print("Trying to submit jobs to condor via dask!")
            fn = os.path.join(os.path.dirname(__file__), "config", "condorconfig.yaml")
            with open(fn) as f:
                defaults = yaml.safe_load(f)
            dask.config.update(dask.config.config, defaults, priority="new")
            cluster = HTCondorCluster()
            cluster.job_extra_directives = {
                "arguments": "DYJets",
                "transfer_input_files": files_needed,
                "output": f"{process}.out",
                "error": f"{process}.err",
                "log": f"{process}.out",
                "stream_error": True,
                "stream_output": True,
            }
            cluster.job_script_prologue = [f'source scripts/condorsetup.sh {process}']
            cluster.scale(jobs=5)
            client = Client(cluster)
            print("One client created!")
            print("===================================")
            print(client)

    return client      

def main():
    client = spawnclient()

    start_time = time.time()

    from config.selectionconfig import runsetting as rs
    from analysis.selutility import Processor
    
    # proc = Processor(rs)

    # if rs.TEST_MODE:
    #     proc.runmultiple(14,16)
    # else:
    #     proc.runmultiple()
        
    print("successfully imported everything!")

    end_time = time.time()
    print(f"Execution time is {end_time - start_time} seconds")

    client.close()

if __name__ == '__main__':
    main()



