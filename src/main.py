# UPDATE TIME: 2024-02-18
# FROM JOY
import os
import time
import logging

PARENT_DIR = os.path.dirname(__file__) 
logging.basicConfig(filename="daskworker.log", 
                    filemode='w', 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
                    level=logging.DEBUG)

from analysis.helper import *
from analysis.spawndask import *

def main():
    start_time = time.time()

    from analysis.selutility import Processor
    print("successfully imported everything!")

    proc = Processor(rs)
    
    client = None 

    if rs.SPAWN_CLIENT:
        client = spawnclient()

    proc.rundata(client)

    end_time = time.time()
    print(f"Execution time is {end_time - start_time} seconds")

    if rs.SPAWN_CLIENT: client.close()

if __name__ == '__main__':
    main()




