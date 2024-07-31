# This file contains the Processor class, which is used to process individual files or filesets.
# The behavior of the Processor class is highly dependent on run time configurations and the event selection class used.
import uproot._util
import uproot, pickle
import pandas as pd
import dask_awkward as dak
import awkward as ak

from utils.filesysutil import *
from .evtselutil import BaseEventSelections

def inittransfer(selname, processname) -> str:
    """Initialize transfer path for condor jobs.
    
    Parameters
    - `selname`: Selection name
    - `processname`: Process name
    
    Returns
    - `transfer`: Transfer path string"""
    condorbase = os.environ.get("CONDOR_BASE", False)
    if condorbase:
        return pjoin(condorbase, selname, processname)
    else:
        raise EnvironmentError("Export condor base directory properly!")

def getTransfer(rtcfg) -> str:
    """Get transfer path for condor jobs

    Parameters
    - `rtcfg`: runsetting object"""
    if rtcfg.get('TRANSFER', True): 
        rtcfg_path = rtcfg.get('TRANSFER_PATH', '')
        if rtcfg_path:
            transfer = rtcfg_path
        else:
            selname = rtcfg.SEL_NAME
            processname = rtcfg.PROCESS_NAME
            transfer = inittransfer(selname, processname)
        return transfer
    else:
        return ''

class Processor:
    """Process individual file or filesets given strings/dicts belonging to one dataset."""
    def __init__(self, rt_cfg, dataset, transferP, evtselclass=BaseEventSelections, **kwargs):
        self._rtcfg = rt_cfg
        self.treename = self.rtcfg.get('TREE_NAME', 'Events')
        self.dataset = dataset
        self.evtsel_kwargs = kwargs
        self.evtselclass = evtselclass
        self.transfer = transferP
        if self.transfer: checkcondorpath(self.transfer)
        self.initdir()

    @property
    def rtcfg(self):
        return self._rtcfg
    
    def initdir(self) -> None:
        """Initialize the output directory and copy directory if necessary.
        If the copy directory is specified, it will be created and checked.
        The output directory will be created and checked."""
        self.outdir = self.rtcfg.OUTPUTDIR_PATH
        if self.rtcfg.COPY_LOCAL: 
            self.copydir = self.rtcfg.get("COPY_DIR", 'temp')
            checkpath(self.copydir, createdir=True)
        checkpath(self.outdir)
    
    def loadfile(self, filename: str, suffix: int, dask_args: dict) -> tuple[ak.Array, bool]:
        """This is a wrapper function around uproot._dask. 
        I am writing this doc to humiliate myself in the future.
        """
        allow_copy = self.rtcfg.get("COPY_LOCAL", False)
        copied = False
        attempts = 0

        while attempts < 2:
            attempts += 1
            try:
                dask_args["files"] = {filename: self.treename}
                if self.rtcfg.get("DELAYED_OPEN", True):
                    events = uproot.dask(**dask_args)
                    break
                else:
                    events = uproot.open(dask_args['files']).arrays()
                    print("Not delayed!")
                    break
            except Exception as e:
                print(f"Failure to load file {filename}")
                print(e)
                if allow_copy and not copied:
                    destpath = pjoin(self.copydir, f"{self.dataset}_{suffix}.root")
                    cpfcondor(filename, destpath)
                    filename = destpath
                    copied = True
                else:
                    events = None
                    break
        return events, copied
    
    def runbatch(self, files, dask_args: dict, indxlst:list=None, **kwargs):
        """Run selections on a batch of files.
        
        Parameters:
        indxlst: list of indices to run on. If None, run on all files."""
        failed = 0 
        if indxlst is None:
            print(f"Expected to see {len(files)} number of outputs")
            for i, file in enumerate(files):
                failed += self.runfile(file, i, dask_args) 
        else:
            print(f"Starting with file number {indxlst[0]}............")
            print(f"Expected to see {len(indxlst)} number of outputs")
            for indx in indxlst:
                failed += self.runfile(files[indx], indx, dask_args, **kwargs)
        return failed

    def runfile(self, filename: str, suffix: int, dask_args: dict, write_npz=False):
        """Run test selections on a single file dict.

        Parameters:
        - write_npz: if write cutflow out
        
        Returns:
        - messages for debugging
        """
        try:
            self.evtsel = self.evtselclass(**self.evtsel_kwargs)
            events, copied = self.loadfile(filename, suffix, dask_args)
            rc = 0
            if events is None: 
                print("Events are not loaded!")
                rc = 1
            else:
                events = self.evtsel(events)

                rc += self.writeCF(suffix, write_npz=write_npz)
                rc += self.writeevts(events, suffix)
                        
                if copied:
                    delfiles(self.copydir)
            del events
        except Exception as e:
            print(f"Error encountered for file index {suffix} in {self.dataset}: {e}")
            rc = 1
        return rc
    
    def writeCF(self, suffix, **kwargs) -> int:
        if kwargs.get('write_npz', False):
            npzname = pjoin(self.outdir, f'cutflow_{suffix}.npz')
            self.evtsel.cfobj.to_npz(npzname)
        cutflow_name = f'{self.dataset}_cutflow_{suffix}.csv'
        checkpath(self.outdir)
        localpath = pjoin(self.outdir, cutflow_name)
        cutflow_df = self.evtsel.cf_to_df() 
        cutflow_df.to_csv(localpath)
        print("Cutflow written to local!")
        if self.transfer:
            if os.path.exists(localpath):
                condorpath = f'{self.transfer}/{cutflow_name}'
                cpcondor(localpath, condorpath)
                os.remove(localpath)
        return 0
    
    def writeevts(self, passed, suffix, **kwargs) -> int:
        """Write the events to a file."""
        if isinstance(passed, dak.lib.core.Array):
            rc = self.writedask(passed, suffix, **kwargs)
        elif isinstance(passed, pd.DataFrame):
            rc = self.writedf(passed, suffix)
        else:
            rc = self.writepickle(passed, suffix, **kwargs)
        if self.transfer:
            transferfiles(self.outdir, self.transfer, filepattern='*', remove=True)
        return rc

    def writedask(self, passed, suffix, fields=None) -> int:
        """Wrapper around uproot.dask_write(),
        transfer all root files generated to a destination location."""
        rc = 0
        delayed = self.rtcfg.get("DELAYED_WRITE", False)
        if fields is None:
            if delayed: uproot.dask_write(passed, destination=self.outdir, tree_name="Events", compute=False, prefix=f'{self.dataset}_{suffix}')
            else: 
                try:
                    uproot.dask_write(passed, destination=self.outdir, tree_name="Events", compute=True, prefix=f'{self.dataset}_{suffix}')
                except Exception as e:
                    print(f"dask_write encountered error {e} for file index {suffix}.")
                    rc = 1
        else:
            rc = 1
        return rc
    
    def writedf(self, passed: pd.DataFrame, suffix) -> int:
        """Writes a pandas DataFrame to a csv file.
        
        Parameters:
        - `passed`: DataFrame to write
        - `suffix`: index to append to filename"""
        outname = pjoin(self.outdir, f'{self.dataset}_output_{suffix}.csv')
        passed.to_csv(outname)
        return 0
        
    def writepickle(self, passed, suffix):
        """Writes results to pkl"""
        finame = pjoin(self.outdir, f"{self.dataset}_{suffix}.pkl")
        with open(finame, 'wb') as f:
            pickle.dump(passed, f)
        return 0