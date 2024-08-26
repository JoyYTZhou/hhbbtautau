# This file contains the Processor class, which is used to process individual files or filesets.
# The behavior of the Processor class is highly dependent on run time configurations and the event selection class used.
import uproot._util
import uproot, pickle
from uproot.writing._dask_write import ak_to_root
import pandas as pd
import dask_awkward as dak
import awkward as ak

from src.utils.filesysutil import *
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

class Processor:
    """Process individual file or filesets given strings/dicts belonging to one dataset."""
    def __init__(self, rt_cfg, dsdict, transferP=None, evtselclass=BaseEventSelections, **kwargs):
        """
        Parameters
        - `ds_dict`: Example dictionary should look like this,
        {"files": {"file1.root": {"steps": [...], "uuid": ...}}, 
         "metadata": {"xsection": 96.978, "shortname": "TTto2L2N"}}
        """
        self._rtcfg = rt_cfg
        self.dsdict = dsdict
        self.dataset = dsdict['metadata']['shortname']
        self.evtsel_kwargs = kwargs
        self.evtselclass = evtselclass
        self.transfer = transferP
        if self.transfer is not None: 
            checkcondorpath(self.transfer)
        self.initdir()

    @property
    def rtcfg(self):
        return self._rtcfg
    
    def initdir(self) -> None:
        """Initialize the output directory and copy directory if necessary.
        If the copy directory is specified, it will be created and checked.
        The output directory will be created and checked."""
        self.outdir = pjoin(self.rtcfg.OUTPUTDIR_PATH, self.dataset)
        if self.rtcfg.COPY_LOCAL: 
            self.copydir = self.rtcfg.get("COPY_DIR", 'temp')
            checkpath(self.copydir, createdir=True)
        checkpath(self.outdir) 
    
    def loadfile_remote(self, fileargs: dict) -> tuple[ak.Array, bool]:
        """This is a wrapper function around uproot._dask.
        
        - `fileargs`: {"files": {filename: fileinfo}}"""
        if self.rtcfg.get("DELAYED_OPEN", True):
            events = uproot.dask(fileargs)
        else:
            events = uproot.open(fileargs['files']).arrays()
            print("Not delayed!")

        return events

    def runfiles(self, fileargs: dict, write_npz=False):
        """Run test selections on file dictionaries.

        Parameters
        - fileargs: dictionary containing file information
        {"file1.root": {"steps": [...], "uuid": ...}, "file2.root": {"steps": [...], "uuid": ...}}
        - write_npz: if write cutflow out
        
        Returns
        - number of failed files
        """
        print(f"Expected to see {len(fileargs)} number of outputs")
        rc = 0
        for filename, fileinfo in fileargs.items():
            try:
                suffix = fileinfo['uuid']
                self.evtsel = self.evtselclass(**self.evtsel_kwargs)
                events = self.loadfile_remote({"files": {filename: fileinfo}})
                if events is not None: 
                    events = self.evtsel(events)
                    self.writeCF(suffix, write_npz=write_npz)
                    self.writeevts(events, suffix)
                else:
                    rc += 1
                del events
            except Exception as e:
                print(f"Error encountered for file index {suffix} in {self.dataset}: {e}")
                rc += 1
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
            condorpath = f'{self.transfer}/{cutflow_name}'
            cpcondor(localpath, condorpath, remove=True)
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
    
    def writeak(self, passed: 'ak.Array', suffix, fields=None) -> int:
        """Writes an awkward array to a root file. Wrapper around ak_to_root."""
        rc = 0
        if fields is None:
            ak_to_root(pjoin(self.outdir, f'{self.dataset}_{suffix}.root'), passed, treename='Events', 
                       compression="ZLIB", compression_level=1, title="", initial_basket_capacity=50, resize_factor=5)

    
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