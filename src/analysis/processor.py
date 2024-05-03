# This file contains the Processor class, which is used to process individual files or filesets.
# The behavior of the Processor class is highly dependent on run time configurations and the event selection class used.
import uproot._util
from utils.filesysutil import *
import uproot
import pickle
from .selutility import BaseEventSelections

class Processor:
    """Process individual file or filesets given strings/dicts belonging to one dataset."""
    def __init__(self, rt_cfg, dataset, evtselclass=BaseEventSelections, **kwargs):
        self._rtcfg = rt_cfg
        self.treename = self.rtcfg.get('TREE_NAME', 'Events')
        self.outdir = self.rtcfg.OUTPUTDIR_PATH
        self.dataset = dataset
        if self.rtcfg.COPY_LOCAL: 
            self.copydir = self.rtcfg.get("COPY_DIR", 'temp')
            checkpath(self.copydir, createdir=True)
        if self.rtcfg.TRANSFER_PATH: 
            checkcondorpath(self.rtcfg.TRANSFER_PATH)
        checkpath(self.outdir)
        self.evtsel = evtselclass(**kwargs) 


    @property
    def rtcfg(self):
        return self._rtcfg
    
    def loadfile(self, filename, suffix):
        """This is a wrapper function around uproot._dask. 
        I am writing this doc to humiliate myself in the future.
        
        :return: The loaded file dict (and error messages if encountered)
        :rtype: dask_awkward.lib.core.Array
        """
        dask_args = {}
        if self.rtcfg.get("STEP_SIZE", False): 
            dask_args["step_size"] = self.rtcfg.STEP_SIZE
        elif self.rtcfg.get("STEP_NO", False):
            dask_args["steps_per_file"] = self.rtcfg.STEP_NO
        else: 
            dask_args["step_size"] = uproot._util.unset

        if self.rtcfg.COPY_LOCAL:
            destpath = pjoin(self.copydir, f"{self.dataset}_{suffix}.root")
            cpfcondor(filename, destpath)
            filename = destpath
        dask_args["files"] = {filename: self.treename}

        try:
            if self.rtcfg.get("DELAYED_OPEN", True):
                print("Delayed!")
                events = uproot.dask(**dask_args)
            else:
                events = uproot.open(dask_args['files']).arrays()
        except OSError as e:
            print(f"Failure to load file {filename}: {e}")
            events = None
        return events
    
    def runfile(self, filename, suffix, write_method='dask', delayed=False, write_npz=False):
        """Run test selections on a single file dict.

        Parameters:
        - filename: path of file to process
        - suffix: append to output file (usually an index to map the filename to a list)
        - write_method: how the output will be saved
        - delayed: if not , output will be computed before saving
        - write_npz: if write cutflow out
        
        Returns:
        - messages for debugging
        """
        events = self.loadfile(filename, suffix)
        rc = 0
        if events is None: 
            rc = 1
            return rc
        events = self.evtsel(events)
        if write_npz:
            npzname = pjoin(self.outdir, f'cutflow_{suffix}.npz')
            self.evtsel.cfobj.to_npz(npzname)
        if write_method == 'dask':
            rc = self.writedask(events, suffix, delayed)
        elif write_method == 'dataframe':
            self.writeobj(events, suffix)
        elif write_method == 'pickle':
            self.writepickle(events, suffix, delayed)
        elif write_method is not None:
            raise ValueError("Write method not supported")

        cutflow_name = f'{self.dataset}_cutflow_{suffix}.csv'
        checkpath(self.outdir)
        localpath = pjoin(self.outdir, cutflow_name)
        cutflow_df = self.evtsel.cf_to_df() 
        cutflow_df.to_csv(localpath)
        print("Cutflow written to local!")

        if self.rtcfg.TRANSFER_PATH:
            if os.path.exists(localpath):
                condorpath = f'{self.rtcfg.TRANSFER_PATH}/{cutflow_name}'
                cpcondor(localpath, condorpath)
                os.remove(localpath)
                
        if self.rtcfg.COPY_LOCAL: 
            delfiles(self.copydir)
        return rc

    def writedask(self, passed, suffix, delayed=True, fields=None) -> int:
        """Wrapper around uproot.dask_write(),
        transfer all root files generated to a destination location."""
        rc = 0
        if fields is None:
            if delayed: uproot.dask_write(passed, destination=self.outdir, tree_name="Events", compute=False, prefix=f'{self.dataset}_{suffix}')
            else: 
                try:
                    uproot.dask_write(passed, destination=self.outdir, tree_name="Events", compute=True, prefix=f'{self.dataset}_{suffix}')
                except ValueError as e:
                    print(f"dask_write encountered error {e} for file index {suffix}.")
                    rc = 1
        else:
            rc = 1

        if self.rtcfg.TRANSFER_PATH:
            transferfiles(self.outdir, self.rtcfg.TRANSFER_PATH, remove=True)
        return rc
        
    def writepickle(self, passed, suffix, delayed):
        finame = pjoin(self.outdir, f"{self.dataset}_{suffix}.pkl")
        with open(finame, 'wb') as f:
            if delayed:
                pickle.dump(passed, f)
            else:
                pickle.dump(passed.compute(), f)
        if self.rtcfg.TRANSFER_PATH:
            cpcondor(finame, pjoin(self.rtcfg.TRANSFER_PATH, f"{self.dataset}_{suffix}.pkl"))
        