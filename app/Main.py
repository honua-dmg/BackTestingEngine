
import Consumers 
import threading
import datetime as dt
import time

import dotenv
import os
import logging
from config import r,STOCKS,FILEPATH,SIMULATION_DATE,s3,EXCHANGE
import simulator
from pymemcache.client.base import Client
memcached_client = Client((os.getenv('MEMCACHE_HOST','memcache'), int(os.getenv("MEMCACHE_PORT",11211))), encoding='utf-8')
PATH = FILEPATH
"""
tail -f /root/stonks/cron.log - to view live - you can also run docker logs -f stonks_app_1
"""

def download_file():
    if not os.path.exists(f'{FILEPATH}/{EXCHANGE}/{STOCKS[0]}/{SIMULATION_DATE}.csv'):
        try:
            s3.download_file(Bucket='kite', Key=f'{EXCHANGE}/{STOCKS[0]}/{SIMULATION_DATE}.csv', Filename=f'{FILEPATH}/{EXCHANGE}/{STOCKS[0]}/{SIMULATION_DATE}.csv')
            print(f"[MAIN] Downloaded {EXCHANGE}:{STOCKS[0]} for {SIMULATION_DATE}")
        except Exception as e:
            raise Exception(f"file not found,choose another date.\n {e}")

def begin():
    """
    This function is called at the beginning of the program. It starts the consumer threads and the producer/simulator.
    """

    r.flushall()
    
    if not SIMULATION_DATE:
        print("[MAIN] Error: SIMULATION_MODE is true but SIMULATION_DATE is not set. Exiting.", flush=True)
        r.set('end', 'true')
        return
    print(f"[MAIN] Starting in SIMULATION mode for date: {SIMULATION_DATE}", flush=True)
    p = simulator.InitialiseSimulator(SIMULATION_DATE)
    consumerThreads = Consumers.start_consumer_threads(PATH, num_consumers=5)
    for thread in consumerThreads:
        thread.join()

def run():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info("Enabled Redis Keyspace Notifications.")
    logging.info("Starting main program")
    logging.info(f"PATH: {PATH}")


    # begin runs irrespective of simulation mode. end runs only when not in simulation mode
    
    logging.info("Starting main program")
    logging.info("Calling begin()")
    download_file()
    try:
        
        begin()
    except KeyboardInterrupt:
        logging.info(" Keyboard Interrupt received. Exiting.")
        r.set('end', 'true')
        
        time.sleep(2)
        logging.info(" Flushing redis")
        r.flushall()
    
    logging.info(" Exiting.")

if __name__ == "__main__":
    """
    Main entry point of the program.
    
    This function is the main entry point of the program. It checks if the market is open and starts the main program.
    """
    run()
