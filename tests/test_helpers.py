import logging

def log_memory(process, stage):
    """Logs memory usage at different stages using a provided psutil.Process() object."""
    mem_usage = process.memory_info().rss / (1024 * 1024)
    logging.debug(f"Memory usage at {stage}: {mem_usage:.2f} MB")
    print(f"Memory usage at {stage}: {mem_usage:.2f} MB")
    return mem_usage