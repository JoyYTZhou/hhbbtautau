import logging
from dask.distributed import get_client
import psutil

def log_memory(process, stage):
    """Logs memory usage at different stages using a provided psutil.Process() object."""
    mem_usage = process.memory_info().rss / (1024 * 1024)
    logging.warning(f"Memory usage at {stage}: {mem_usage:.2f} MB")
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
        logging.warning(f"Dask workers memory: {worker_memory}")
    else:
        logging.warning("No memory information available from Dask workers")

    logging.warning(f"System memory: {psutil.virtual_memory().percent}%")

def setup_logging():
    # Enhanced logging format for debugging
    logging.getLogger().handlers.clear()
    logging.basicConfig(
        filename='debug.log',
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )
    # Also show logs in console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    logging.getLogger().addHandler(console_handler)
    logging.getLogger("uproot").setLevel(logging.WARNING)
    logging.getLogger("dask").setLevel(logging.DEBUG)
    logging.getLogger("distributed").setLevel(logging.DEBUG)
    logging.getLogger("fsspec").setLevel(logging.WARNING)

def log_memory_snapshot(snapshot, message):
    top_stats = snapshot.statistics('lineno')
    logging.debug(f"Memory snapshot: {message}")
    for stat in top_stats[:10]:
        logging.debug(stat)
    
def log_memory_diff(snapshot1, snapshot2, message):
    top_stats = snapshot2.compare_to(snapshot1, 'lineno')
    logging.debug(f"[ Memory differences after {message} ]")
    for stat in top_stats[:10]:
        logging.debug(stat)