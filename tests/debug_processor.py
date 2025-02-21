import tracemalloc, logging, psutil, gc, dask, os
from uproot.writing._dask_write import ak_to_root
import dask_awkward as dak
import awkward as ak
from src.analysis.processor import Processor

pjoin = os.path.join

class DebugProcessor(Processor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def runfiles(self, write_npz=False, readkwargs={}, writekwargs={}, **kwargs) -> int:
        print(f"Expected to see {len(self.dsdict['files'])} outputs")
        rc = 0
        for filename, fileinfo in self.dsdict["files"].items():
            print(filename)
            try:
                suffix = fileinfo['uuid']
                self.evtsel = self.evtselclass(**self.evtsel_kwargs)
                remote_load = self.rtcfg.get("REMOTE_LOAD", True)
                events = self.loadfile_remote(fileargs={"files": {filename: fileinfo}}, **readkwargs) if remote_load else self.loadfile_local(fileargs={"files": {filename: fileinfo}}, **readkwargs)
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

        process = psutil.Process()
        
        def log_memory(stage):
            mem_usage = process.memory_info().rss / (1024 * 1024)
            logging.debug(f"Memory usage at {stage}: {mem_usage:.2f} MB")
            print(f"Memory usage at {stage}: {mem_usage:.2f} MB")
            return mem_usage
        
        log_array_info(passed, "input array")

        rc = 0
        delayed = self.rtcfg.get("DELAYED_WRITE", False)

        if not parquet:
            write_options = {
                "initial_basket_capacity": 512,  # Smaller initial basket size
                "resize_factor": 1.5,           # Smaller growth factor
                "compression": "ZLIB",
                "compression_level": 1,         # Lower compression level
            }
            try:
                # Step 1: Compute the dask array
                mem_before_compute = log_memory("before compute")
                logging.debug("Computing dask array...")
                
                if hasattr(passed, 'npartitions') and passed.npartitions > 1:
                    computed_chunks = []
                    for i in range(passed.npartitions):
                        chunk = passed.partitions[i]
                        logging.debug(f"Computing chunk {i}/{passed.npartitions}")
                        print(f"Computing chunk {i}/{passed.npartitions}")
                        computed_chunk = dask.compute(chunk)[0]
                        gc.collect()
                        computed_chunks.append(computed_chunk)
                        log_memory(f"after computing chunk {i}")
                        del chunk, computed_chunk
                        gc.collect()

                    computed_data = ak.concatenate(computed_chunks)
                else:
                    computed_data = dask.compute(passed)[0]
            
                mem_after_compute = log_memory("after compute")
                logging.debug(f"Memory difference after compute: {mem_after_compute - mem_before_compute:.2f} MB")

                # Step 2: Write to disk
                logging.debug("Writing to disk...")
                mem_before_write = log_memory("before write")

                output_path = pjoin(self.outdir, f'{self.dataset}_{suffix}.root')
                ak_to_root(output_path, computed_data, tree_name="Events", title="", 
                           counter_name=lambda counted: 'n' + counted, field_name=lambda outer, inner: inner if outer == "" else outer + "_" + inner,
                           storage_options=None,
                           **write_options)
                
                mem_after_write = log_memory("after write")
                logging.debug(f"Memory difference after write: {mem_after_write - mem_before_write:.2f} MB")

                del computed_data
                gc.collect()
                log_memory("after cleanup")

            except MemoryError as e:
                print(f"Memory error during processing: {e}")
                print("Current memory state:")
                print(f"Available system memory: {psutil.virtual_memory().available / (1024**3):.2f} GB")
                print(f"Process memory usage: {process.memory_info().rss / (1024**3):.2f} GB")
                rc = 1
            except Exception as e:
                print(f"Error during processing: {e}")
                rc = 1
        else:
            # if delayed:
            #     uproot.dask_write(passed, destination=self.outdir, 
            #                     tree_name="Events", compute=False,
            #                     prefix=f'{self.dataset}_{suffix}',
            #                    write_options)

            dak.to_parquet(passed, destination=self.outdir,
                        prefix=f'{self.dataset}_{suffix}')
        return rc

    def log_memory_diff(self, snapshot1, snapshot2, message):
        top_stats = snapshot2.compare_to(snapshot1, 'lineno')
        logging.debug(f"[ Memory differences after {message} ]")
        for stat in top_stats[:10]:
            logging.debug(stat)