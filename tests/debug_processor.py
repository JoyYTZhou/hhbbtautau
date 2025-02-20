import tracemalloc, uproot
import logging, psutil, gc
import dask_awkward as dak
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

    def writedask(self, passed, suffix, parquet=False, fields=None) -> int:
        """Wrapper around uproot.dask_write(),
        transfer all root files generated to a destination location.

        Parameters:
        - `parquet`: if True, write to parquet instead of root"""
        def log_array_info(arr, name="array"):
            try:
                logging.debug(f"{name} info:")
                logging.debug(f"Number of partitions: {arr.npartitions if hasattr(arr, 'npartitions') else 'N/A'}")
                logging.debug(f"Shape: {arr.shape if hasattr(arr, 'shape') else 'N/A'}")
                logging.debug(f"DTTypes: {arr.dtypes if hasattr(arr, 'dtypes') else 'N/A'}")
            except Exception as e:
                logging.debug(f"Could not get {name} info: {e}")
        
        log_array_info(passed, "input array")

        rc = 0
        delayed = self.rtcfg.get("DELAYED_WRITE", False)

        # Get memory usage before writing
        process = psutil.Process()
        mem_before = process.memory_info().rss / (1024 * 1024)  # Convert to MB
        print(f"Memory usage before writing: {mem_before:.2f} MB")

        if not parquet:
            write_options = {
                "initial_basket_capacity": 512,  # Smaller initial basket size
                "resize_factor": 1.5,           # Smaller growth factor
                "compression": "ZLIB",
                "compression_level": 1,         # Lower compression level
            }

            if delayed:
                uproot.dask_write(passed, destination=self.outdir, 
                                tree_name="Events", compute=False,
                                prefix=f'{self.dataset}_{suffix}',
                                **write_options)
            else:
                try:
                    # Process in smaller chunks if possible
                    if hasattr(passed, 'npartitions') and passed.npartitions > 1:
                        for i in range(passed.npartitions):
                            chunk = passed.partitions[i]
                            chunk_suffix = f"{suffix}_part{i}"
                            
                            # Monitor memory before chunk
                            mem_before_chunk = process.memory_info().rss / (1024 * 1024)
                            print(f"Memory before chunk {i}: {mem_before_chunk:.2f} MB")
                            
                            uproot.dask_write(chunk, destination=self.outdir,
                                            tree_name="Events", compute=True,
                                            prefix=f'{self.dataset}_{chunk_suffix}',
                                            **write_options)
                            
                            # Force garbage collection after each chunk
                            gc.collect()
                            
                            # Monitor memory after chunk
                            mem_after_chunk = process.memory_info().rss / (1024 * 1024)
                            print(f"Memory after chunk {i}: {mem_after_chunk:.2f} MB")
                    else:
                        uproot.dask_write(passed, destination=self.outdir,
                                        tree_name="Events", compute=True,
                                        prefix=f'{self.dataset}_{suffix}',
                                        **write_options)
                except MemoryError:
                    print(f"dask_write encountered error: MemoryError for file index {suffix}.")
                    rc = 1
        else:
            dak.to_parquet(passed, destination=self.outdir,
                        prefix=f'{self.dataset}_{suffix}')

        # Get memory usage after writing
        mem_after = process.memory_info().rss / (1024 * 1024)
        print(f"Memory usage after writing: {mem_after:.2f} MB")
        print(f"Memory difference: {mem_after - mem_before:.2f} MB")

        return rc

    def log_memory_diff(self, snapshot1, snapshot2, message):
        top_stats = snapshot2.compare_to(snapshot1, 'lineno')
        logging.debug(f"[ Memory differences after {message} ]")
        for stat in top_stats[:10]:
            logging.debug(stat)