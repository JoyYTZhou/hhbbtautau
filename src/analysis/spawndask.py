from dask.distributed import Client, LocalCluster
from dask.distributed import as_completed
import json as json
import traceback, os, random

from .custom import switch_selections
from .processor import Processor, getTransfer
from utils.filesysutil import glob_files, get_xrdfs_file_info, check_missing, checkpath, pjoin
from config.selectionconfig import runsetting as rs
from config.selectionconfig import dasksetting as daskcfg

evtselclass = switch_selections(rs.SEL_NAME)

parent_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

datapath = pjoin(parent_directory, 'data', 'data.json')
with open(datapath, 'r') as data:
    realmeta = json.load(data)

transferP = getTransfer(rs)

def div_list(original_list, chunk_size):
    """Divide a list into smaller lists of given size."""
    for i in range(0, len(original_list), chunk_size):
        yield original_list[i:i + chunk_size]

def filterResume(metadata, tsferP=transferP) -> dict:
    """Resume jobs from last checkpoint"""
    if not tsferP or checkpath(tsferP, createdir=False) != 0:
        return metadata
    
    loaded = {}
    for ds, ds_info in metadata.items():
        print(f"Checking {ds} ========================================================")
        fileno = len(ds_info['filelist'])
        missing_cutflow = set(check_missing(f'{ds}_cutflow', fileno, tsferP, endpattern='.csv'))
        missing_output = set(check_missing(f'{ds}', fileno, tsferP, endpattern='.root'))
        
        if missing_cutflow:
            print(f"Missing cutflow tables for these files: {missing_cutflow}!")
        if missing_output:
            print(f"Missing output for these files: {missing_output} ")
        
        missing_indices = sorted(list(missing_cutflow.union(missing_output)))
        
        if missing_indices:
            loaded[ds] = {'resumeindx': missing_indices, 'filelist': ds_info['filelist']}
    return loaded
    
class JobRunner:
    def __init__(self, jobfile, eventSelection=evtselclass) -> None:
        self.selclass = eventSelection
        self._ds = None
        with open(jobfile, 'r') as job:
            self._loaded = json.load(job)
    
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
            if use_futures:
                futures = self.submitfutures(client, filelist, resumeindx)
                result = process_futures(futures)
            else:
                print("Submit jobs in loops!")
                self.submitloops(filelist, resumeindx)
        return 0

    def submitloops(self, filelist, indx) -> int:
        """Put file processing in loops, i.e. one file by one file.
        Usually used for large file size."""
        
        print(f"Processing {self.ds}...")
        proc = Processor(rs, self.ds, transferP, self.selclass)
        failed = proc.runbatch(filelist, indx)
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
    def __init__(self, jobpath) -> None:
        self.inpath = rs.INPUTFILE_PATH
        self.tsferP = transferP
        self.jobpath = jobpath
        checkpath(jobpath)

    def writejobs(self, filterfunc=filterResume) -> None:
        """Write job parameters to json file"""
        if self.inpath.endswith('.json'):
            self.skimjobs(filterfunc)
        elif self.inpath.startswith('/store/user/'):
            loaded = realmeta[rs.PROCESS_NAME]
            for dataset in loaded.keys():
                loaded[dataset]['filelist'] = glob_files(self.inpath, startpattern=dataset, endpattern='.root')
            if filterfunc is not None: loaded = filterfunc(loaded, self.tsferP)
            if loaded: json.dump(loaded, pjoin(self.jobpath, f'{rs.PROCESS_NAME}_job.json'))
            else: print("All the input files have been processed!")
        else:
            raise TypeError("Check INPUTFILE_PATH in runsetting.toml. It's not of a valid format!")
        
    def skimjobs(self, filterfunc, batch_size=10) -> None:
        inputdatap = pjoin(parent_directory, self.inpath)
        with open(inputdatap, 'r') as samplepath:
            loaded = json.load(samplepath)
        dslist = list(loaded.keys()) 
        for i, dskey in enumerate(dslist):
            dsloaded = {dskey: loaded[dskey]}
            if filterfunc is not None: 
                filtered = filterfunc(dsloaded, self.tsferP)
            resumeindx = filtered.get('resumeindx', [j for j in range(len(dsloaded[dskey]['filelist']))])
            indx_gen = div_list(resumeindx, batch_size)
            for j, indx_list in enumerate(indx_gen):
                dsloaded['resumeindx'] = indx_list
                with open(pjoin(self.jobpath, f'{rs.PROCESS_NAME}_{i}_job_{j}.json'), 'w') as fp:
                    json.dump(dsloaded, fp)

def checkjobs(tsferP=transferP) -> None:
    """Check if there are files left to be run."""
    loaded = loadmeta(filterResume)
    print(f"Checking {tsferP} for output files!")
    if loaded:
        for ds in loaded.keys():
            if 'resumeindx' in loaded[ds]:
                filelen = len(loaded[ds]['resumeindx'])
            else:
                filelen = len(loaded[ds]['filelist'])
            print(f"There are {filelen} files left to be run in {ds}.")
    else:
        print("All the files have been processed!")

def sampleloaded(loaded) -> int:
    """Sample a random file from loaded metadata."""
    firstitem = loaded[next(iter(loaded))]
    randindx = random.randint(0, len(firstitem['filelist']))
    splitname = '//store'
    filename = firstitem['filelist'][randindx]
    if not filename.startswith('/store'):
        splitted = filename.split(splitname, 1)
        redir = splitted[0]
        finame = '/store' + splitted[1]
        size, mod_time = get_xrdfs_file_info(finame, redir)
    else:
        size, mod_time = get_xrdfs_file_info(filename)
    return size
    
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

