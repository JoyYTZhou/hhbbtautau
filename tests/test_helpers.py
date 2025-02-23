import logging
from dask.distributed import get_client
import psutil

def log_memory(process, stage):
    """Logs memory usage at different stages using a provided psutil.Process() object."""
    mem_usage = process.memory_info().rss / (1024 * 1024)
    logging.debug(f"Memory usage at {stage}: {mem_usage:.2f} MB")
    print(f"Memory usage at {stage}: {mem_usage:.2f} MB")
    return mem_usage

def log_dask_status():
    client = get_client()
    workers = client.scheduler_info()['workers']

    worker_memory = []
    for worker in workers.values():
        try:
            if 'memory' in worker:
                worker_memory.append(worker['memory'])
        except Exception as e:
            logging.warning(f"Could not get memory info from worker: {e}")

    if worker_memory:
        logging.debug(f"Dask workers memory: {worker_memory}")
    else:
        logging.debug("No memory information available from Dask workers")

    logging.debug(f"System memory: {psutil.virtual_memory().percent}%")