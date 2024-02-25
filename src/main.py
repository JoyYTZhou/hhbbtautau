# UPDATE TIME: 2024-02-18
# FROM JOY
import os
import time
import logging

PARENT_DIR = os.path.dirname(__file__) 

logging.basicConfig(filename="daskworker.log", 
                    filemode='w', 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
                    level=logging.INFO)

from config.selectionconfig import runsetting as rs

def main():
    start_time = time.time()
    
    from analysis.spawndask import runfutures

    if rs.SPAWN_CLIENT:
        from analysis.spawndask import spawnclient
        client = spawnclient()
    else:
        client = None
    
    runfutures(client)

    end_time = time.time()
    print(f"Execution time is {end_time - start_time} seconds")

    if rs.SPAWN_CLIENT: client.close()

if __name__ == '__main__':
    main()




