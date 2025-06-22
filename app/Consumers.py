
import os
import dotenv
import json
import threading
import pandas as pd

import datetime as dt
import time
from collections import defaultdict
from config import r,STOCKS,CONFIG_DIR
import math
import logging
import csv
class Consumer():
    def __init__(self,directory,num_consumers):
        """
        Initializes a Consumer object with the given directory and number of consumers.
        
        Args:
            directory (str): The directory where the CSV files are stored.
            num_consumers (int): The number of consumers to be used.
        """
        
        self.directory = directory
        self.num_consumers = num_consumers
        self.nse = self.tokenStockMapping("NSE")
        self.bse = self.tokenStockMapping("BSE")

        self.consumers = {}
        self.date = dt.datetime.strftime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5.5),"%Y-%m-%d")
        # Synchronization primitives for safe Rebalancing
        self.rebalance_interval = 60
        self.rebalance_lock = threading.Lock()
        self.rebalance_condition = threading.Condition(self.rebalance_lock) # still don't really understand how this works.
        self.rebalance_in_progress = False
        self.paused_consumers_count = 0
        self.r  = r
        self.r.set('end','false')   
        self.count_lock = threading.Lock()
        self.count = 0

        # we'll make consumer groups and oen consumer for each stock
        for stock in STOCKS:
            try:
                self.r.xgroup_create(name=stock,groupname=stock,mkstream=True)
                self.r.xgroup_createconsumer(name=stock,group=stock,consumername=stock)
            except Exception as e:
                #consumer already exists
                continue

    

    def tokenStockMapping(self,exchange):
        """
        Maps tokens to their corresponding stock symbols.
        
        Args:
            exchange (str): The exchange name ('NSE' or 'BSE').
        
        Returns:
            dict: A dictionary mapping tokens to their stock symbols.
        """
        df = pd.read_csv(os.path.join(CONFIG_DIR, f"{exchange}.csv"))
        return dict(zip( df['instrument_token'],df['tradingsymbol']))

    def ConvertToken(self,token):
        """
        Converts a token to a stock symbol.
        
        Args:
            token (int): The token to convert.
        
        Returns:
            str: The stock symbol corresponding to the token.
        """
        if token in self.nse.keys():
            return f"NSE:{self.nse[token]}"
        elif token in self.bse.keys():
            return f"BSE:{self.bse[token]}"

    def do(self,msg_data):
        """        
        with open('test.csv', 'a') as f:
            # Assuming msg_data is a dictionary of field-value pairs
            f.write(f"{msg_data}\n")
            f.flush()"""
        pass

    def CSVConsumer(self,id):
        """
        Consumes data from Redis and saves it to CSV files.
        
        Args:
            id (int): The ID of the consumer.
        """
        
        logging.info(f"Consumer {id} started.")
        while self.r.get('end')!='true':
            # Check for rebalancing flag
            with self.rebalance_lock:
                if self.rebalance_in_progress:
                    logging.info(f"Consumer {id} pausing for rebalancing.")
                    self.paused_consumers_count += 1
                    self.rebalance_condition.notify()
                    while self.rebalance_in_progress:
                        self.rebalance_condition.wait()
                    logging.info(f"Consumer {id} resuming after rebalancing.")
                
            offset_keys = [stock for stock in self.consumers[id]]

                # Poll Redis for new messages in the stream for this stock
            for stock in offset_keys:

                # try to claim messages first
                _, claimed_messages, _ = self.r.xautoclaim(stock,stock,stock,min_idle_time=0,start_id='0-0')
                if claimed_messages:
                        for msg_id,msg_data in claimed_messages:
                            self.do(msg_data)
                            with self.count_lock:
                                self.count += 1
                            self.r.xack(stock, stock, msg_id)
                # if no messages are claimed, poll for new messages
                new_messages = self.r.xreadgroup(groupname=stock,consumername=stock,streams={stock: ">"},block=10)
            
                if new_messages:
                    for _,stream_messages in new_messages:
                        for msg_id, msg_data in stream_messages:
                            try:
                                self.do(msg_data) 
                                with self.count_lock:
                                    self.count += 1
                                self.r.xack(stock, stock, msg_id)
                            except Exception as e:
                                logging.error(f"[ERROR] Failed to process tick {msg_id} for {stock}: {e}")
                                continue
            with self.count_lock:
                if self.count % 1000 == 0:
                    logging.info(f"Consumer {id} processed {self.count} ticks.")

        logging.info('ending csvWorker')

    def saveData(self):
        """
        initialises the CSV files and starts the CSVConsumer threads.
        assigns each thread with an even number of stocks at random.
        """


        stock_keys = STOCKS
        No_stocks = len(stock_keys)
        stocksPerConsumer = math.ceil(No_stocks/self.num_consumers)
        threads = []
        stocks = stock_keys # completely redundant, there's absolutely no need to reassign the stocks list, but this is proof this was written by a human low on sleep :)
        assignments = {}
        for i in range(0,No_stocks,stocksPerConsumer):
            cid = math.ceil(i/stocksPerConsumer)
            assignments[cid] = stocks[i:i+stocksPerConsumer]
            thread = threading.Thread(target=self.CSVConsumer,args=(cid,),name=f'CSVCONSUMER_{cid}')
            threads.append(thread)

        with self.rebalance_lock:
            self.consumers = assignments

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()
        
    def jobscheduler(self):
        """
        Rebalances the stocks between the CSVConsumer threads.
        
        This function is called every hour to rebalance the stocks between the CSVConsumer threads.
        It counts the number of lines in the CSV files for the current date and assigns the stocks to the threads
        in a way that minimizes the total number of lines in each thread.
        """
        with self.rebalance_lock:
            logging.info(f"[REBALANCE] Starting rebalancing cycle at {dt.datetime.now()}")
            self.rebalance_in_progress = True

            # Wait until all consumers have paused
            while self.paused_consumers_count < self.num_consumers:
                logging.info(f"[REBALANCE] Waiting for consumers to pause... ({self.paused_consumers_count}/{self.num_consumers})")
                self.rebalance_condition.wait(timeout=10) # Wait for a signal

            logging.info("[REBALANCE] All consumers paused. Rebalancing...")

        # --- Rebalancing Logic (can be outside the main lock) ---
        all_stocks = STOCKS
        logging.info(f"[REBALANCE] COUNT DONE: {dt.datetime.now()} Number of stocks: {len(all_stocks)}")
        bse = []

        for stock in all_stocks:
            try:
                count = self.r.xlen(stock)
                bse.append((stock, count))
            except Exception as e:
                logging.error(f"[ERROR] Could not get xlen for {stock}: {e}")
        bse.sort(key=lambda x: -x[1])
        logging.info(f"[REBALANCE] COUNT DONE: {dt.datetime.now()} Number of stocks: {len(bse)}")
        num_consumers = self.num_consumers  # safer than len(self.consumers)
        new_assignments = defaultdict(list)
        totals = [0] * num_consumers
        for i, (stock, count) in enumerate(bse[:num_consumers]):
            new_assignments[i].append(stock)
            totals[i] += count
        for stock, count in bse[num_consumers:]:
            min_index = totals.index(min(totals))
            new_assignments[min_index].append(stock)
            totals[min_index] += count
        with self.rebalance_lock:
            self.consumers = dict(new_assignments)
            for cid, stocks in self.consumers.items():
                total_count = totals[cid]  # total count assigned to this consumer
                logging.info(f"[REBALANCE] Assigned {total_count} stocks to consumer {cid}, {stocks}")
        
            self.rebalance_in_progress = False
            self.paused_consumers_count = 0
            self.rebalance_condition.notify_all() # Wake up all waiting consumers
            logging.info("[REBALANCE] Rebalance complete. Consumers resuming.")

    def start_thread_monitor(self, check_interval=6):
        """
        Starts a thread that monitors the CSVConsumer threads.
        
        This function is called when the Consumer object is initialized.
        It starts a thread that monitors the CSVConsumer threads and restarts them if they are down.
        """

        #TODO implement thread condition to check if all threads are done, in which case end. 
        def monitor():
            while self.r.get('end')!='true':
                time.sleep(check_interval)
                active = {t.name for t in threading.enumerate()}
                with self.rebalance_lock:
                    for cid in self.consumers:
                        tname = f"CSVCONSUMER_{cid}"
                        if tname not in active:
                            logging.info(f"[Monitor] {tname}, responsible for :\n\t{self.consumers[cid]}\n is down. Restarting...")

                            thread = threading.Thread(target=self.CSVConsumer, args=(cid,), name=tname)
                            thread.start()
        threading.Thread(target=monitor, daemon=True).start()
    
    def start_scheduler(self):
        """
        Starts a thread that runs the jobscheduler function every hour.
        
        This function is called when the Consumer object is initialized.
        It starts a thread that runs the jobscheduler function every 10 mins.
        """
        def loop():
            while self.r.get('end')!='true' :
                time.sleep(self.rebalance_interval)
                self.jobscheduler()
        threading.Thread(target=loop, daemon=True).start()

def start_consumer_threads(directory,num_consumers=5):
        """
        Starts the following methods in separate threads within the same process:
        - start_thread_monitor: launches internal monitoring thread(s)
        - start_scheduler: launches internal scheduling thread(s)
        - saveData: launches worker threads for data saving
        """
        consumer = Consumer(directory, num_consumers)

        def run_thread_monitor():
            consumer.start_thread_monitor()

        def run_scheduler():
            consumer.start_scheduler()

        def run_save_data():
            consumer.saveData()

        # Create threads
        t_monitor = threading.Thread(target=run_thread_monitor, name="ThreadMonitorStarter")
        t_scheduler = threading.Thread(target=run_scheduler, name="SchedulerStarter")
        t_save_data = threading.Thread(target=run_save_data, name="SaveDataStarter")


        # Start core threads

        #t_monitor.start()
        #t_scheduler.start()
        t_save_data.start()
        # Start optional threads

        #consumer.start_cleanup_thread()

        return [t_save_data]
