
import Consumers 
import threading
import datetime as dt
import time

import dotenv
import os
import logging
from config import r,STOCKS,FILEPATH
import simulator
from pymemcache.client.base import Client
memcached_client = Client((os.getenv('MEMCACHE_HOST','memcache'), int(os.getenv("MEMCACHE_PORT",11211))), encoding='utf-8')
PATH = FILEPATH
"""
tail -f /root/stonks/cron.log - to view live - you can also run docker logs -f stonks_app_1
"""



def begin():
    """
    This function is called at the beginning of the program. It starts the consumer threads and the producer/simulator.
    """

    r.flushall()
    simulation_date = os.getenv("SIMULATION_DATE")
    if not simulation_date:
        print("[MAIN] Error: SIMULATION_MODE is true but SIMULATION_DATE is not set. Exiting.", flush=True)
        r.set('end', 'true')
        return
    print(f"[MAIN] Starting in SIMULATION mode for date: {simulation_date}", flush=True)
    p = simulator.InitialiseSimulator(simulation_date)
    consumerThreads = Consumers.start_consumer_threads(PATH, num_consumers=5)
    for thread in consumerThreads:
        thread.join()


if __name__ == "__main__":
    """
    Main entry point of the program.
    
    This function is the main entry point of the program. It checks if the market is open and starts the main program.
    """
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
    try:
        begin()
    except KeyboardInterrupt:
        logging.info("[MAIN] Keyboard Interrupt received. Exiting.")
        r.set('end', 'true')
        time.sleep(2)
        print("[MAIN] Flushing redis")
        r.flushall()
    
    logging.info("[MAIN] Exiting.")
