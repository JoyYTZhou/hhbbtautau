from config.selectionconfig import settings as sel_cfg
from .helper import *
import pickle
from .selutility import BaseEventSelections, Object
from .custom import prelimEvtSel, fineEvtSel

output_cfg = sel_cfg.signal.outputs

class Processor:
    """Process individual file or filesets given strings/dicts belonging to one dataset.
    """
    def __init__(self, rt_cfg, dataset, evtselclass=BaseEventSelections):
        self._rtcfg = rt_cfg
        self.treename = self.rtcfg.TREE_NAME
        self.outdir = self.rtcfg.OUTPUTDIR_PATH
        self.dataset = dataset
        if self.rtcfg.COPY_LOCAL: checkpath(self.rtcfg.COPY_DIR)
        if self.rtcfg.TRANSFER: 
            checkcondorpath(self.rtcfg.TRANSFER_PATH)
        checkpath(self.outdir)
        self.defselections()
        self.evtsel = evtselclass(self.lepcfg, self.jetcfg, self.channelname) 

    @property
    def rtcfg(self):
        return self._rtcfg
    
    def defselections(self):
        self.lepcfg = sel_cfg.signal[f'channel{self.rtcfg.CHANNEL_INDX}'].selections
        self.jetcfg = sel_cfg.signal.commonsel
        self.channelname = sel_cfg.signal[f'channel{self.rtcfg.CHANNEL_INDX}'].name
        
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
            cproot(filename, destpath)
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

        passed = self.evtsel.select(events, return_veto=False)
        if write_npz:
            npzname = pjoin(self.outdir, f'cutflow_{suffix}_{self.channelname}.npz')
            evtsel.cfobj.to_npz(npzname)
        if write_method == 'dask':
            self.writedask(passed, self.channelname, suffix, delayed)
        elif write_method == 'dataframe':
            self.writeobj(passed, self.channelname, suffix)
        elif write_method == 'pickle':
            self.writepickle(passed, suffix, delayed)
            pass
        elif write_method == None:
            pass
        else:
            raise ValueError("Write method not supported")

        cutflow_name = f'{self.dataset}_cutflow_{suffix}.csv'
        localpath = pjoin(self.outdir, cutflow_name)
        cutflow_df = evtsel.cf_to_df() 
        cutflow_df.to_csv(localpath)

        del evtsel, cutflow_df, events, passed

        if self.rtcfg.TRANSFER:
            condorpath = f'{self.rtcfg.TRANSFER_PATH}/{cutflow_name}'
            cpcondor(localpath, condorpath, is_file=True)
        msg.append(f"file {filename} processed successfully!")
        if self.rtcfg.COPY_LOCAL: delfiles(self.rtcfg.COPY_DIR)

        return '\n'.join(msg)

    def writedask(self, passed, prefix, suffix, delayed=True, fields=None):
        """Wrapper around uproot.dask_write(),
        transfer all root files generated to a destination location."""
        if fields is None:
            dir_name = pjoin(self.outdir, prefix)
            checkpath(dir_name)
            if delayed: uproot.dask_write(passed, destination=dir_name, compute=False, prefix=f'{self.dataset}_{prefix}_{suffix}')
            else: 
                uproot.dask_write(passed, destination=dir_name, compute=True, prefix=f'{self.dataset}_{prefix}_{suffix}')
        else:
            pass

        if self.rtcfg.TRANSFER:
            transferfiles(dir_name, self.rtcfg.TRANSFER_PATH)
            shutil.rmtree(dir_name)
        
    def writeobj(self, passed, index, suffix):
        """This can be further simplified.I do not like this function...
        Write computed awk array selected in sel_cfg to csv files."""
        chcfg = self.channelsel[index]
        electron = Object("Electron", passed, output_cfg.Electron, chcfg.selections.electron)
        muon = Object("Muon", passed, output_cfg.Muon, chcfg.selections.muon)
        tau = Object("Tau", passed, output_cfg.Tau, chcfg.selections.tau)

        edf = electron.to_daskdf()
        mdf = muon.to_daskdf()
        tdf = tau.to_daskdf()

        obj_df = pd.concat([edf, mdf, tdf], axis=1)
        del edf, mdf, tdf

        obj_path = pjoin(self.outdir, f"{chcfg.name}{suffix}.csv")
        obj_df.to_csv(obj_path, index=False)

        if self.rtcfg.TRANSFER:
            dest_path = pjoin(self.rtcfg.TRANSFER_PATH, f"{chcfg.name}{suffix}.csv")
            cpcondor(obj_path, dest_path, is_file=True)
    
    def writepickle(self, passed, suffix, delayed):
        finame = pjoin(self.outdir, f"{self.dataset}_{suffix}.pkl")
        with open(finame, 'wb') as f:
            if delayed:
                pickle.dump(passed, f)
            else:
                pickle.dump(passed.compute(), f)
        if self.rtcfg.TRANSFER:
            cpcondor(finame, pjoin(self.rtcfg.TRANSFER_PATH, f"{self.dataset}_{suffix}.pkl"))
        