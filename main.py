#!python3

import ingestion
import load
import time

if __name__ == "__main__":
    start_time=time.time()
    ingestion.main()
    load.main()
    print("[ETL] overall ETL duration: {0:.2f} seconds".format(time.time() - start_time))
    

