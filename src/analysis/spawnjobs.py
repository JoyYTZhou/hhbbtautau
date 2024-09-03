from dask.distributed import Client, LocalCluster
from dask.distributed import as_completed
import json as json
import gzip, glob, traceback, os

from .custom import switch_selections
from .processor import Processor
from src.utils.filesysutil import glob_files, check_missing, checkpath, pjoin
from config.selectionconfig import runsetting as rs
from config.selectionconfig import dasksetting as daskcfg

evtselclass = switch_selections(rs.SEL_NAME)

p_dirname = os.path.dirname

src_dir = p_dirname(p_dirname(os.path.abspath(__file__)))
base_dir = p_dirname(src_dir)
data_dir = pjoin(base_dir, 'data')

datapath = pjoin(data_dir, 'availableQuery.json')
with open(datapath, 'r') as data:
    realmeta = json.load(data)

transferP = rs.get("TRANSFER_PATH", None)

def get_fi_prefix(filepath):
    return os.path.basename(filepath).split('.')[0]

def div_list(original_list, chunk_size):
    """Divide a list into smaller lists of given size."""
    for i in range(0, len(original_list), chunk_size):
        yield original_list[i:i + chunk_size]

def filterExisting(dsdata: 'dict', outputpattern='*.root', tsferP=transferP) -> bool:
    """Update dsdata on files that need to be processed for a MC dataset based on the existing output files and cutflow tables.
    
    Parameters
    - `dsdata`: A dictionary of dataset information with keys 'files', 'metadata', 'filelist'

    Return
    - bool: True if some files need to be processed, False otherwise. 
    """
    if not tsferP or checkpath(tsferP, createdir=False) != 0:
        return True
    
    filelist = dsdata['files'].keys()
    fileno = len(filelist)
    shortname = dsdata["metadata"]["shortname"]
    missing_cutflow = set(check_missing(f'{shortname}_cutflow*.csv', fileno, tsferP))
    missing_output = set(check_missing(f'{shortname}{outputpattern}', fileno, tsferP))
        
    if missing_cutflow:
        print(f"Missing cutflow tables for these files: {missing_cutflow}!")
    if missing_output:
        print(f"Missing output for these files: {missing_output} ")
    
    missing_indices = sorted(list(missing_cutflow.union(missing_output)))
        
    if missing_indices:
        dsdata["resumeindx"] = missing_indices
        return True
    else: return False
    
class JobRunner:
    def __init__(self, jobfile, eventSelection=evtselclass) -> None:
        self.selclass = eventSelection
        with open(jobfile, 'r') as job:
            self._loaded = json.load(job)
            grp_name = get_fi_prefix(jobfile)
    
    @property
    def ds(self):
        return self._ds
    @ds.setter
    def ds(self, dsname):
        self._ds = dsname
        
    def submitjobs(self, client) -> int:
        """Run jobs based on client settings.
        If a valid client is found and future mode is true, submit simultaneously run jobs.
        If not, fall back into a loop mode. Note that even in this mode, any dask computations will be managed by client explicitly or implicitly.
        """
        loaded = self._loaded 
        if not loaded: 
            print("Double check the job json file!")
            print("All the files have been processed for this dataset!")
            return 0

        use_futures = client is not None and daskcfg.get('SPAWN_FUTURE', False)
        for ds, dsitems in loaded.items():
            self.ds = ds
            resumeindx = dsitems.get('resumeindx', None)
            filelist = dsitems['filelist']
            daskargs = self.get_meta_daskargs(filelist[0], client)
            if use_futures:
                futures = self.submitfutures(client, filelist, resumeindx)
                result = process_futures(futures)
            else:
                print("Submit jobs in loops!")
                self.submitloops(filelist, resumeindx, daskargs)
        return 0

    def submitloops(self, filelist, indx, daskargs) -> int:
        """Put file processing in loops, i.e. one file by one file.
        Usually used for large file size."""
        
        print(f"Processing {self.ds}...")
        proc = Processor(rs, self.ds, transferP, self.selclass)
        failed = proc.runbatch(filelist, daskargs, indx)
        return failed
    
    def submitfutures(self, client, filelist, indx) -> list:
        """Submit jobs as futures to client.
        
        Parameters
        - `client`: Dask client

        Returns
        list: List of futures for each file in the dataset.
        """
        futures = []
        def job(fn, i):
            proc = Processor(rs, self.ds, transferP, self.selclass) 
            rc = proc.runfile(fn, i)
            return rc
        if indx is None:
            futures.extend([client.submit(job, fn, i) for i, fn in enumerate(filelist)])
        else:
            futures.extend([client.submit(job, filelist[i]) for i in indx])
        return futures

class JobLoader():
    def __init__(self, jobpath, datapath=pjoin(data_dir, 'preprocessed')) -> None:
        self.inpath = datapath
        checkpath(self.inpath, createdir=False, raiseError=True)
        self.tsferP = transferP
        self.jobpath = jobpath
        checkpath(jobpath)

    def writejobs(self) -> None:
        """Write job parameters to json file"""
        datafile = glob.glob(pjoin(self.inpath, '*.json.gz'))
        for file in datafile:
            self.prepjobs(file)
        
    def prepjobs(self, inputdatap, batch_size=15) -> bool:
        with gzip.open(inputdatap, 'rt') as samplepath:
            grp_name = get_fi_prefix(inputdatap)
            loaded = json.load(samplepath)
        for ds, dsdata in loaded.items():
            print(f"===============Preparing job files for {ds}========================")
            need_process = filterExisting(dsdata, tsferP=self.tsferP)
            if need_process:
                resumeindx = dsdata.get('resumeindx', [j for j in range(len(dsdata["files"]))])
                indx_gen = div_list(resumeindx, batch_size)
                shortname = dsdata['metadata']['shortname']
                for j, indx_list in enumerate(indx_gen):
                    dsdata['resumeindx'] = indx_list
                    finame = pjoin(self.jobpath, f'{grp_name}_{shortname}_job_{j}.json')
                    with open(finame, 'w') as fp:
                        json.dump(dsdata, fp)
                    print("Job file created: ", finame)
                return True
            else:
                print(f"All the files have been processed for {ds}! No job files are needed!")
                return False
    
def process_futures(futures, results_file='futureresult.txt', errors_file='futureerror.txt'):
    """Process a list of Dask futures.
    :param futures: List of futures returned by client.submit()
    :return: A list of results from successfully completed futures.
    """
    processed_results = []
    errors = []
    for future in as_completed(futures):
        try:
            if future.exception():
                error_msg = f"An error occurred: {future.exception()}"
                print(future.traceback())
                print(f"Traceback: {traceback.extract_tb(future.traceback())}")
                print(error_msg)
                errors.append(error_msg)
            else:
                result = future.result()
                processed_results.append(result)
        except Exception as e:
            error_msg = f"Error processing future result: {e}"
            print(error_msg)
            errors.append(error_msg)
    with open(results_file, 'w') as f:
        for result in processed_results:
            f.write(str(result) + '\n')
    if errors:
        with open(errors_file, 'w') as f:
            for error in errors:
                f.write(str(error) + '\n')
    return processed_results, errors

def spawnLocal():
    """Spawn dask client for local cluster"""
    # size_mb = size/1024/1024
    cluster = LocalCluster(processes=daskcfg.get('SPAWN_PROCESS', False), threads_per_worker=daskcfg.get('THREADS_NO', 2))
    cluster.adapt(minimum=1, maximum=4)
    client = Client(cluster)
    print("successfully created a dask client in local cluster!")
    print("===================================")
    print(client)
    return client

def spawnclient(default=False):
    """Spawn appropriate client based on runsetting."""
    if not daskcfg.SPAWN_CONDOR:
        client = spawnLocal()
    else:
        client = spawnCondor(default)
    return client 

def spawnCondor(default=False):
    """Spawn dask client for condor cluster"""
    from lpcjobqueue import LPCCondorCluster

    print("Trying to submit jobs to condor via dask!")

    if default:
        cluster = LPCCondorCluster(ship_env=True)
        cluster.adapt(maximum=3)
        print(cluster.job_script())
    else:
        condor_args = {"ship_env": True, 
                    "processes": daskcfg.PROCESS_NO,
                    "cores": daskcfg.CORE_NO,
                    "memory": daskcfg.MEMORY,
                    "disk": daskcfg.DISK
                    }
        cluster = LPCCondorCluster(**condor_args)
        cluster.job_extra_directives = {
            'output': 'dask_output.$(ClusterId).$(ProcId).out',
            'error': 'daskr_error.$(ClusterId).$(ProcId).err',
            'log': 'dask_log.$(ClusterId).log',
        }
        cluster.adapt(minimum=daskcfg.MIN_WORKER, maximum=daskcfg.MAX_WORKER)
        print(cluster.job_script())

    client = Client(cluster)
    print("One client created in LPC Condor!")
    print("===================================")
    print(client)

    return client

