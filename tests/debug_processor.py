import tracemalloc
import logging
from src.analysis.processor import Processor

class DebugProcessor(Processor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def runfiles(self, write_npz=False, **kwargs):
        print(f"Expected to see {len(self.dsdict['files'])} outputs")
        rc = 0
        for filename, fileinfo in self.dsdict["files"].items():
            print(filename)
            try:
                suffix = fileinfo['uuid']
                self.evtsel = self.evtselclass(**self.evtsel_kwargs)
                remote_load = self.rtcfg.get("REMOTE_LOAD", True)
                events = self.loadfile_remote(fileargs={"files": {filename: fileinfo}}) if remote_load else self.loadfile_local(fileargs={"files": {filename: fileinfo}})
                if events is not None:
                    events = self.evtsel(events)

                    # Take memory snapshot before writeCF
                    snapshot_before_writeCF = tracemalloc.take_snapshot()
                    logging.debug("Took snapshot before writeCF")

                    self.writeCF(suffix, write_npz=write_npz)

                    # Take memory snapshot after writeCF
                    snapshot_after_writeCF = tracemalloc.take_snapshot()
                    logging.debug("Took snapshot after writeCF")
                    self.log_memory_diff(snapshot_before_writeCF, snapshot_after_writeCF, "writeCF")

                    # Take memory snapshot before writeevts
                    snapshot_before_writeevts = tracemalloc.take_snapshot()
                    logging.debug("Took snapshot before writeevts")

                    self.writeevts(events, suffix, **kwargs)

                    # Take memory snapshot after writeevts
                    snapshot_after_writeevts = tracemalloc.take_snapshot()
                    logging.debug("Took snapshot after writeevts")
                    self.log_memory_diff(snapshot_before_writeevts, snapshot_after_writeevts, "writeevts")

                else:
                    rc += 1
                del events
            except Exception as e:
                print(f"Error encountered for file index {suffix} in {self.dataset}: {e}")
                rc += 1
                import gc
                gc.collect()
            if not remote_load: self.filehelper.remove_files(self.copydir)
        return rc

    def log_memory_diff(self, snapshot1, snapshot2, message):
        top_stats = snapshot2.compare_to(snapshot1, 'lineno')
        logging.debug(f"[ Memory differences after {message} ]")
        for stat in top_stats[:10]:
            logging.debug(stat)