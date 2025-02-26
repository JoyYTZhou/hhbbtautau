import tracemalloc, logging, psutil, dask, uproot, gc, threading
from uproot.writing._dask_write import ak_to_root
import concurrent.futures
import awkward as ak
import dask_awkward as dak
from src.analysis.processor import Processor, writeCF, process_file

from src.utils.filesysutil import pjoin, XRootDHelper
from src.utils.testutils import log_memory


class DebugProcessor(Processor):
    write_skim_semaphore = threading.Semaphore(2)
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def writedask(self, passed, suffix, **kwargs):
        with self.write_skim_semaphore:
            return write_skimmed(passed, self.outdir, self.dataset, suffix, self.rtcfg, **kwargs)

    def run_skims(self, write_npz=False, max_workers=2, readkwargs={}, writekwargs={}, **kwargs) -> int:
        logging.debug(f"Expected to see {len(self.dsdict['files'])} outputs")
        rc = 0
        import psutil
        process = psutil.Process()

        available_mem = psutil.virtual_memory().available / (1024**3)
        logging.debug(f"Available system memory: {available_mem:.2f} GB")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            log_memory(process, "before processing")
            future_loaded = parallel_copy_and_load(
                fileargs={"files": self.dsdict["files"]},
                copydir=self.copydir,
                executor=executor,
                rtcfg=self.rtcfg,
                read_args=readkwargs)
            
            future_cf, future_writes, future_passed = [], [], {}
            
            for future in concurrent.futures.as_completed(future_loaded.values()):
                filename = next(f for f, future in future_loaded.items() if future == future)
                
                try: 
                    events, suffix = future.result()
                    future_passed[suffix] = executor.submit(self.evtselclass(**self.evtsel_kwargs).callevtsel, events)
                except Exception as e:
                    logging.exception(f"Error copying and loading {filename}: {e}")
                    gc.collect()
                
            for future in concurrent.futures.as_completed(future_passed.values()):
                suffix = next(s for s, f in future_passed.items() if f == future)

                try:
                    log_memory(process, f"before writing for file {suffix}")
                    passed, evtsel_state = future.result()

                    future_cf.append(executor.submit(writeCF, evtsel_state, suffix, self.outdir, self.dataset))
                    future_writes.append(executor.submit(self.writedask, passed, suffix, **writekwargs))
                except Exception as e:
                    logging.exception(f"Error processing {suffix}: {e}")
            
            cutflow_files = []
            for future in concurrent.futures.as_completed(future_cf):
                cutflow_files.append(future.result())
                del future
                gc.collect()
            
            for future in concurrent.futures.as_completed(future_writes):
                rc += future.result()
                del future
                gc.collect()
            
            del future_cf, future_writes, future_passed, future_loaded
            log_memory(process, "after processing + writing")
            gc.collect()
        
            if self.transfer:
                for cutflow_file in cutflow_files:
                    self.filehelper.transfer_files(self.outdir, self.transfer, filepattern=cutflow_file, remove=True)

            if not self.rtcfg.get("REMOTE_LOAD", True):
                self.filehelper.remove_files(self.copydir)
        return rc
    

def write_skimmed(passed, outdir, dataset, suffix, rtcfg, parquet=False, fields=None) -> int:
    """
    Write skimmed data to ROOT or parquet files.

    Args:
        passed: The data to write (dask_awkward or awkward array)
        outdir: Output directory path
        dataset: Dataset name
        suffix: File suffix
        rtcfg: Runtime configuration dictionary
        parquet: If True, write to parquet format instead of ROOT
        fields: Fields to write (optional)

    Returns:
        int: Return code (0 for success, 1 for failure)
    """
    process = psutil.Process()
    rc = 0
    delayed = rtcfg.get("DELAYED_WRITE", False)

    if not parquet:
        write_options = {
            "initial_basket_capacity": 50,  # Smaller initial basket size
            "resize_factor": 1.5,           # Smaller growth factor
            "compression": "ZLIB",
            "compression_level": 1,         # Lower compression level
        }
        try:
            log_memory(process, "before compute")
            logging.debug("Computing dask array...")

            if hasattr(passed, 'npartitions'):
                length_calcs = [dask.delayed(len)(passed.partitions[i]) for i in range(passed.npartitions)]
                lengths = dask.compute(*length_calcs)

                has_zero_lengths = any(l == 0 for l in lengths)

                if not has_zero_lengths:
                    logging.debug("No zero-arrays found, using uproot.dask_write directly")
                    uproot.dask_write(
                        passed,
                    destination=outdir,
                        tree_name="Events",
                        compute=not delayed,
                    prefix=f'{dataset}_{suffix}',
                        **write_options
                        )
                    logging.debug(f"Finished writing {dataset}_{suffix}.root")
                else:
                    logging.debug("Found zero-length partitions, filtering them out")
                    # Filter out zero-length partitions
                    valid_indices = [i for i, l in enumerate(lengths) if l > 0]
                    if not valid_indices:
                        logging.debug("No valid partitions found, skipping write")
                        del passed, length_calcs, lengths
                        return None
                    logging.debug(f"Valid indices: {valid_indices}")
                    # valid_partitions = dak.concatenate([passed.partitions[i] for i in valid_indices])
                    # computed_data = dask.compute(valid_partitions)[0]
                    computed_partitions = [dask.compute(passed.partitions[i])[0] for i in valid_indices]
                    computed_data = ak.concatenate(computed_partitions)
                    output_path = pjoin(outdir, f'{dataset}_{suffix}.root')
                    ak_to_root(output_path, computed_data, tree_name="Events", title="",
                        counter_name=lambda counted: 'n' + counted,
                        field_name=lambda outer, inner: inner if outer == "" else outer + "_" + inner,
                        storage_options=None,
                        **write_options)
                    logging.debug(f"Finished writing {output_path}")
                del valid_indices, computed_partitions, computed_data, passed
            else: 
                logging.error("Passed object does not have npartitions attribute, skipping write")
        except MemoryError as e:
            logging.error(f"Memory error during processing: {e}")
            logging.debug("Current memory state:")
            logging.debug(f"Available system memory: {psutil.virtual_memory().available / (1024**3):.2f} GB")
            logging.debug(f"Process memory usage: {process.memory_info().rss / (1024**3):.2f} GB")
            rc = 1
        except Exception as e:
            logging.error(f"Error during processing: {e}")
            rc = 1
        finally:
            if hasattr(passed, 'unpersist'):
                passed.unpersist()
    gc.collect()
    return rc

    # def process_file_worker(self, readkwargs={}, writekwargs={}, **kwargs):
    #     """Worker that processes copied files."""
    #     while True:
    #         file_info = self.copy_queue.get()
    #         if file_info is None:
    #             self.copy_queue.task_done()
    #             break

    #         try:
    #             logging.debug(f"Processing {file_info['local_path']}")
                
    #             # Create new fileargs for the copied file
    #             new_filename = f"{file_info['local_path']}:Events"
    #             new_fileargs = {
    #                 "files": {
    #                     new_filename: self.dsdict['files'][file_info['original']]
    #                 }
    #             }

    #             # Process the file
    #             events = (uproot.dask(**new_fileargs, **readkwargs) 
    #                      if self.rtcfg.get("DELAYED_OPEN", True) 
    #                      else uproot.open(new_filename).arrays(**kwargs))

    #             if events is not None:
    #                 self.evtsel = self.evtselclass(**self.evtsel_kwargs)
    #                 events = self.evtsel(events)
    #                 self.writeCF(file_info['suffix'], write_npz=kwargs.get('write_npz', False))
    #                 self.writeevts(events, file_info['suffix'], **kwargs)
                
    #             # Cleanup
    #             del events
    #             os.remove(file_info['local_path'])
                
    #         except Exception as e:
    #             logging.error(f"Error processing file {file_info['local_path']}: {e}")
    #         finally:
    #             self.copy_queue.task_done()
    #             gc.collect()

    # async def pipeline_files(self, **kwargs):
    #     """Pipeline file copying and processing."""
    #     logging.info(f"Processing {len(self.dsdict['files'])} files")
    #     rc = 0

    #     # Start the processor thread
    #     processor_thread = Thread(
    #         target=self.process_file_worker,
    #         kwargs=kwargs,
    #         daemon=True
    #     )
    #     processor_thread.start()

    #     # Create copy tasks
    #     copy_tasks = []
    #     for filename, fileinfo in self.dsdict["files"].items():
    #         if filename.endswith(":Events"):
    #             filename = filename.split(":Events")[0]
            
    #         copy_tasks.append(
    #             self.copy_file_worker(filename, fileinfo['uuid'])
    #         )

    #     # Run copy tasks
    #     try:
    #         await asyncio.gather(*copy_tasks)
    #     except Exception as e:
    #         logging.error(f"Error in copy tasks: {e}")
    #         rc += 1
    #     finally:
    #         # Signal end of copying
    #         self.copy_queue.put(None)

    #     # Wait for processor to finish
    #     self.copy_queue.join()
    #     processor_thread.join()

    #     return rc 

    # def runfiles(self, write_npz=False, **kwargs):
    #     """Run the pipeline."""
    #     return asyncio.run(self.pipeline_files(write_npz=write_npz, **kwargs))
