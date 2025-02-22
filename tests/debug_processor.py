import tracemalloc, logging, psutil, dask, os, uproot
from uproot.writing._dask_write import ak_to_root
import concurrent.futures
from threading import Thread
import dask_awkward as dak
from src.analysis.processor import Processor

from src.utils.filesysutil import pjoin
from tests.test_helpers import log_memory 

def compute_dask_array(passed):
    """Compute the dask array and handle zero-length partitions."""
    process = psutil.Process()

    log_memory(process, "before compute")
    logging.debug("Computing dask array...")

    if hasattr(passed, 'npartitions'):
        passed = passed.persist()
        log_memory(process, "after persist")

        length_calcs = [dask.delayed(len)(passed.partitions[i]) for i in range(passed.npartitions)]
        persisted_lengths = dask.persist(*length_calcs)
        lengths = dask.compute(*persisted_lengths)

        has_zero_lengths = any(l == 0 for l in lengths)

        if not has_zero_lengths:
            logging.debug("No zero-arrays found, using uproot.dask_write directly")
            return passed
        else:
            logging.debug("Found zero-length partitions, filtering them out")
            valid_indices = [i for i, l in enumerate(lengths) if l > 0]
            if not valid_indices:
                logging.debug("No valid partitions found, skipping write")
                return None
            else:
                logging.debug(f"Valid indices: {valid_indices}")
                valid_partitions = [passed.partitions[i] for i in valid_indices]
                valid_data = dak.concatenate(valid_partitions)
                computed_data = dask.compute(valid_data)[0]
                return computed_data

class DebugProcessor(Processor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def writedask(self, passed, suffix, parquet=False, fields=None) -> int:
        process = psutil.Process()

        rc = 0
        delayed = self.rtcfg.get("DELAYED_WRITE", False)

        if not parquet:
            write_options = {
                "initial_basket_capacity": 50,  # Smaller initial basket size
                "resize_factor": 1.5,           # Smaller growth factor
                "compression": "ZLIB",
                "compression_level": 1,         # Lower compression level
            }
            try:
                mem_before_compute = log_memory(process, "before compute")
                logging.debug("Computing dask array...")
                
                if hasattr(passed, 'npartitions'):
                    mem_before_persist = log_memory(process, "before persist")
                    logging.debug("Persisting dask array...")
                    passed = passed.persist()
                    mem_after_persist = log_memory(process, "after persist")
                 
                    length_calcs = [dask.delayed(len)(passed.partitions[i]) for i in range(passed.npartitions)]
                    persisted_lengths = dask.persist(*length_calcs)  # Keeps it lazy

                    # Compute when needed
                    lengths = dask.compute(*persisted_lengths)
                    
                    has_zero_lengths = any(l == 0 for l in lengths)

                    if not has_zero_lengths:
                        logging.debug("No zero-arrays found, using uproot.dask_write directly")
                        uproot.dask_write(
                            passed,
                            destination=self.outdir,
                            tree_name="Events",
                            compute=not delayed,
                            prefix=f'{self.dataset}_{suffix}',
                            **write_options
                            )
                    else:
                        logging.debug("Found zero-length partitions, filtering them out")
                        # Filter out zero-length partitions
                        valid_indices = [i for i, l in enumerate(lengths) if l > 0]
                        if not valid_indices:
                            logging.debug("No valid partitions found, skipping write")
                            return 0
                        else:
                            logging.debug(f"Valid indices: {valid_indices}")
                            # Create new dask array with only valid partitions
                            valid_partitions = [passed.partitions[i] for i in valid_indices]
                            valid_data = dak.concatenate(valid_partitions).persist()

                            computed_data = dask.compute(valid_data)[0]
                            output_path = pjoin(self.outdir, f'{self.dataset}_{suffix}.root') 
                            ak_to_root(output_path, computed_data, tree_name="Events", title="", 
                            counter_name=lambda counted: 'n' + counted, field_name=lambda outer, inner: inner if outer == "" else outer + "_" + inner,
                            storage_options=None,
                            **write_options)
                    # # Compute all chunk lengths simultaneously
                    # chunk_counts = dask.compute(*[
                    #     passed.partitions[i].count() 
                    #     for i in range(passed.npartitions)
                    # ])
                    # has_zero_chunks = any(count == 0 for count in chunk_counts)
                    
                    # if not has_zero_chunks:
                    #     logging.debug("No zero-arrays found, using uproot.dask_write directly")
                    #     uproot.dask_write(
                    #         passed,
                    #         destination=self.outdir,
                    #         tree_name="Events",
                    #         compute=not delayed,
                    #         prefix=f'{self.dataset}_{suffix}',
                    #         **write_options
                    #     )                
                    # computed_chunks = []
                    # for i in range(passed.npartitions):
                    #     chunk = passed.partitions[i]
                    #     logging.debug(f"Computing chunk {i}/{passed.npartitions}")
                    #     print(f"Computing chunk {i}/{passed.npartitions}")
                    #     computed_chunk = dask.compute(chunk)[0]
                    #     computed_chunks.append(computed_chunk)
                    #     log_memory(f"after computing chunk {i}")
                    #     gc.collect()

                    # computed_data = ak.concatenate(computed_chunks)
                # else:
                    # computed_data = dask.compute(passed)[0]
            
                # mem_after_compute = log_memory("after compute")
                # logging.debug(f"Memory difference after compute: {mem_after_compute - mem_before_compute:.2f} MB")

                # # Step 2: Write to disk
                # logging.debug("Writing to disk...")
                # mem_before_write = log_memory("before write")

                # output_path = pjoin(self.outdir, f'{self.dataset}_{suffix}.root')
                # ak_to_root(output_path, computed_data, tree_name="Events", title="", 
                           # counter_name=lambda counted: 'n' + counted, field_name=lambda outer, inner: inner if outer == "" else outer + "_" + inner,
                           # storage_options=None,
                           # **write_options)
                
                # mem_after_write = log_memory("after write")
                # logging.debug(f"Memory difference after write: {mem_after_write - mem_before_write:.2f} MB")

                # del computed_data
                # gc.collect()
                # log_memory("after cleanup")

            except MemoryError as e:
                print(f"Memory error during processing: {e}")
                print("Current memory state:")
                print(f"Available system memory: {psutil.virtual_memory().available / (1024**3):.2f} GB")
                print(f"Process memory usage: {process.memory_info().rss / (1024**3):.2f} GB")
                rc = 1
            except Exception as e:
                print(f"Error during processing: {e}")
                rc = 1
            finally:
                if hasattr(passed, 'unpersist'):
                     passed.unpersist()
        else:
            dak.to_parquet(passed, destination=self.outdir,
                        prefix=f'{self.dataset}_{suffix}')
        return rc

    def log_memory_diff(self, snapshot1, snapshot2, message):
        top_stats = snapshot2.compare_to(snapshot1, 'lineno')
        logging.debug(f"[ Memory differences after {message} ]")
        for stat in top_stats[:10]:
            logging.debug(stat)


    
    # async def copy_file_worker(self, filename: str, suffix: str):
    #     """Worker that copies files to local storage."""
    #     try:
    #         dest_file = pjoin(self.copydir, f"copy_{suffix}.root")
    #         logging.debug(f"Copying {filename} to {dest_file}")
            
    #         # Perform the copy operation
    #         await asyncio.get_event_loop().run_in_executor(
    #             None,
    #             XRootDHelper.copy_local,
    #             filename,
    #             dest_file
    #         )
            
    #         # Put the copied file info into the queue
    #         self.copy_queue.put({
    #             'local_path': dest_file,
    #             'suffix': suffix,
    #             'original': filename
    #         })

    #         logging.debug(f"Finished copying {filename}")
            
    #     except Exception as e:
    #         logging.error(f"Error copying file {filename}: {e}")
    #         self.copy_queue.put(None)

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
