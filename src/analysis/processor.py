# This file contains the Processor class, which is used to process individual files or filesets.
# The behavior of the Processor class is highly dependent on run time configurations and the event selection class used.
from utils.filesysutil import *
import uproot
import pickle
from .selutility import BaseEventSelections
import shutil

class Processor:
    """Process individual file or filesets given strings/dicts belonging to one dataset."""
    def __init__(self, rt_cfg, dataset, evtselclass=BaseEventSelections, **kwargs):
        self._rtcfg = rt_cfg
        self.treename = self.rtcfg.TREE_NAME
        self.outdir = self.rtcfg.OUTPUTDIR_PATH
        self.dataset = dataset
        if self.rtcfg.COPY_LOCAL: checkpath(self.rtcfg.COPY_DIR)
        if self.rtcfg.TRANSFER: 
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
        msg = []
        user_step_size = uproot._util.unset if not self.rtcfg.STEP_SIZE else self.rtcfg.STEP_SIZE
        if self.rtcfg.COPY_LOCAL:
            destpath = pjoin(self.rtcfg.COPY_DIR, f"{self.dataset}_{suffix}.root")
            cpfcondor(filename, destpath)
            try:
                events = uproot.dask(
                    files={destpath: self.treename},
                    step_size=user_step_size
                )
            except OSError as e:
                msg.append(f"Failed again to load file after copying: {e}")
                events = None
        else:
            events = uproot.dask(
                files={filename: self.treename},
                step_size=user_step_size
            ) 
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
        msg = []
        events = self.loadfile(filename, suffix)
        passed = self.evtsel(events)
        if write_npz:
            npzname = pjoin(self.outdir, f'cutflow_{suffix}.npz')
            self.evtsel.cfobj.to_npz(npzname)
        if write_method == 'dask':
            self.writedask(passed, suffix, delayed)
        elif write_method == 'dataframe':
            self.writeobj(passed, suffix)
        elif write_method == 'pickle':
            self.writepickle(passed, suffix, delayed)
            pass
        elif write_method == None:
            pass
        else:
            raise ValueError("Write method not supported")

        cutflow_name = f'{self.dataset}_cutflow_{suffix}.csv'
        checkpath(self.outdir)
        localpath = pjoin(self.outdir, cutflow_name)
        cutflow_df = self.evtsel.cf_to_df() 
        cutflow_df.to_csv(localpath)

        del cutflow_df, events, passed

        if self.rtcfg.TRANSFER:
            condorpath = f'{self.rtcfg.TRANSFER_PATH}/{cutflow_name}'
            cpcondor(localpath, condorpath)
        msg.append(f"file {filename} processed successfully!")
        if self.rtcfg.COPY_LOCAL: delfiles(self.rtcfg.COPY_DIR)

        return '\n'.join(msg)

    def writedask(self, passed, suffix, delayed=True, fields=None):
        """Wrapper around uproot.dask_write(),
        transfer all root files generated to a destination location."""
        if fields is None:
            if delayed: uproot.dask_write(passed, destination=self.outdir, tree_name="Events", compute=False, prefix=f'{self.dataset}_{suffix}')
            else: 
                uproot.dask_write(passed, destination=self.outdir, tree_name="Events", compute=True, prefix=f'{self.dataset}_{suffix}')
        else:
            pass

        if self.rtcfg.TRANSFER:
            transferfiles(self.outdir, self.rtcfg.TRANSFER_PATH)
            shutil.rmtree(self.outdir)
        
    def writepickle(self, passed, suffix, delayed):
        finame = pjoin(self.outdir, f"{self.dataset}_{suffix}.pkl")
        with open(finame, 'wb') as f:
            if delayed:
                pickle.dump(passed, f)
            else:
                pickle.dump(passed.compute(), f)
        if self.rtcfg.TRANSFER:
            cpcondor(finame, pjoin(self.rtcfg.TRANSFER_PATH, f"{self.dataset}_{suffix}.pkl"))
        