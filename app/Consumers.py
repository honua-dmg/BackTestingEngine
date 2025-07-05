
import os
import dotenv
import json
import threading
import pandas as pd
import numpy as np
import datetime as dt
import time
from collections import defaultdict
from config import r,STOCKS,CONFIG_DIR,DECTECTION_TYPE,VOLUME,TEST,GRAPH
import math
import logging
from StockAnalyser import Cumulative_Support
import pyqtgraph as pg

class Consumer():
    def __init__(self,directory,num_consumers):
        """
        Initializes a Consumer object with the given directory and number of consumers.
        
        Args:
            directory (str): The directory where the CSV files are stored.
            num_consumers (int): The number of consumers to be used.
        """
        stocks  = json.load(open(os.path.join(CONFIG_DIR, "stocks.json")))[TEST]
        self.directory = directory
        self.num_consumers = num_consumers
        self.nse = self.tokenStockMapping("NSE")
        self.bse = self.tokenStockMapping("BSE")

        self.consumers = {}
        self.analysers = {}
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
        for stock in stocks:
            for exchange in stocks[stock]:
                self.analysers[f'{exchange}:{stock}'] = Cumulative_Support(detection_type=DECTECTION_TYPE,vol=VOLUME)

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

        # in reality in msg_data we'll get the token, so we'll have to convert hat ourselves manualy
        #stock = self.ConvertToken(msg_data['token'])
        stock = msg_data['stonk']
        self.analysers[stock].parse(msg_data)
        #print(stock,np.float32(msg_data['last_price']))
        

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



def graph(instance):
    
    


    def addHeatMap(plot,data):
        # Create a colormap with transparency
        colors = [
            (1, (0, 255, 0, 200)), # Green with light alpha
            (0, (255, 0, 0, 200))    # Red with light alpha
        ]
        cmap = pg.ColorMap(pos=np.array([c[0] for c in colors]), color=np.array([c[1] for c in colors], dtype=np.ubyte))
        img = pg.ImageItem(data)
        img.setOpacity(.5)
        lut = cmap.getLookupTable(nPts=256, alpha=True)  # Enable alpha
        img.setLookupTable(lut)
        plot.addItem(img)
        return img
    
    def addlinePlot(plot,linewidth,data:pd.Series,name,color=None):
        x,y = getXY(data)
        if color==None:
            return plot.plot(x,y,pen=pg.mkPen( width=linewidth,name=name))
        return plot.plot(x,y,pen=pg.mkPen(color=color, width=linewidth,name=name))
    
    def getXY(data: pd.Series):
        if data is None or data.empty or data.shape == 0:
            return [], []
        if isinstance(data, pd.DataFrame):
        # Take first column automatically
            data = data.iloc[:, 0]
        x = instance.ltpDf.index.to_numpy()
        
        # Convert to numeric if data is not already float or int
        if data.dtypes not in [np.float64, np.int64]:  
            y = pd.to_numeric(data, errors='coerce').to_numpy()
        else:
            y = data.to_numpy()

        # Ensure x and y have the same length
        min_len = min(len(x), len(y))
        x, y = x[:min_len], y[:min_len]  

        # Create mask and apply it safely
        mask = ~np.isnan(y)
        mask = mask[:min_len]  # Ensure mask has the same length as x and y

        return x[mask], y[mask]  # Filter out NaN values
                        
    def update():
        # update hmaps:
        instance.hmap_buy_LH.setImage(instance.lowHighdf[0].to_numpy().T, autoLevels=False)
        instance.hmap_buy_HL.setImage(instance.highLowdf[0].to_numpy().T, autoLevels=False)
        instance.hmap_sell_LH.setImage(instance.lowHighdf[1].to_numpy().T, autoLevels=False)
        instance.hmap_sell_HL.setImage(instance.highLowdf[1].to_numpy().T, autoLevels=False)

        min_ltp = instance.ltpDf['ltp'].min()
        n = len(instance.ltpDf)
        m = len(instance.aggDf)
        for h in [instance.hmap_buy_LH, instance.hmap_buy_HL, instance.hmap_sell_LH, instance.hmap_sell_HL]:
            h.setRect(0, min_ltp, n, m)

        # update line plots:
        x = instance.ltpDf.index.to_numpy()
        y_ltp = instance.ltpDf['ltp'].to_numpy()
        instance.line_ltp_buy.setData(x, y_ltp)
        instance.line_ltp_sell.setData(x, y_ltp)
        #update buy trend lines
        x, y = getXY(instance.lowHighMaxes[0])
        instance.line_upper_1_buy.setData(x, y)
        x, y = getXY(instance.HighlowMaxes[0])
        instance.line_lower_1_buy.setData(x, y)
        #update sell trend lines:
        x, y = getXY(instance.lowHighMaxes[0])
        instance.line_upper_1_sell.setData(x, y)
        x, y = getXY(instance.HighlowMaxes[0])
        instance.line_lower_1_sell.setData(x, y)
        #update voll_diff:
        ##
        
        x,y = getXY(instance.voldiff_50[0])
        instance.vol_diff_50.setData(x, y)
        x,y = getXY(instance.voldiff_20[0])
        instance.vol_diff_20.setData(x, y)
        """
        x,y = getXY(instance.voldiff_200[0])
        instance.vol_diff_200.setData(x, y)
        x,y = getXY(instance.voldiff_300[0])
        instance.vol_diff_300.setData(x, y)
        """
        ##
    while instance.ltpDf.empty:
        time.sleep(1)


    app = pg.mkQApp()

    win_buy = pg.GraphicsLayoutWidget(title="Buy Heatmap & Trends")
    win_buy.setBackground("white")
    plot_buy = win_buy.addPlot(title="Buy Side")
    plot_buy.addLegend()
    win_buy.show()

    win_sell = pg.GraphicsLayoutWidget(title="Sell Heatmap & Trends")
    win_sell.setBackground("white")
    plot_sell = win_sell.addPlot(title="Sell Side")
    plot_sell.addLegend()
    win_sell.show()

    win_diff = pg.GraphicsLayoutWidget(title="Volume Difference")
    win_diff.setBackground("white")
    plot_diff = win_diff.addPlot(title="Volume Difference")
    plot_diff.addLegend()
    win_diff.show()

    #add ltp to both plots:
    instance.line_ltp_buy = addlinePlot(plot_buy, linewidth=2, data=instance.ltpDf['ltp'], name='LTP', color=(0, 0, 255))
    instance.line_ltp_sell = addlinePlot(plot_sell, linewidth=2, data=instance.ltpDf['ltp'], name='LTP', color=(0, 0, 255))

    # add heatmaps:
    instance.hmap_buy_LH = addHeatMap(plot_buy, instance.lowHighdf[0].to_numpy().T)
    instance.hmap_buy_HL = addHeatMap(plot_buy, instance.highLowdf[0].to_numpy().T)
    instance.line_upper_1_buy = addlinePlot(plot_buy, linewidth=4, data=instance.lowHighMaxes[0], name='Buy Uptrend', color='#097969')
    instance.line_lower_1_buy = addlinePlot(plot_buy, linewidth=4, data=instance.HighlowMaxes[0], name='Buy Downtrend', color='#fbd604')

    #sell heatmaps:
    instance.hmap_sell_LH = addHeatMap(plot_sell, instance.lowHighdf[1].to_numpy().T)
    instance.hmap_sell_HL = addHeatMap(plot_sell, instance.highLowdf[1].to_numpy().T)
    instance.line_upper_1_sell = addlinePlot(plot_sell, linewidth=4, data=instance.lowHighMaxes[1], name='Sell Uptrend', color='#5F9EA0')
    instance.line_lower_1_sell = addlinePlot(plot_sell, linewidth=4, data=instance.HighlowMaxes[1], name='Sell Downtrend', color='#fdd750')

    #voll_diff:
    instance.vol_diff_50 = addlinePlot(plot_diff, linewidth=4, data=instance.voldiff_50, name='VolDiff_50', color='#fdd750')
    instance.vol_diff_20 = addlinePlot(plot_diff, linewidth=4, data=instance.voldiff_20, name='VolDiff_20', color='#097969')
    #instance.vol_diff_300 = addlinePlot(plot_diff, linewidth=4, data=instance.voldiff_300, name='VolDiff_300', color=(0, 0, 255))

    # Adjust axis colors for visibility
    for p in [plot_buy, plot_sell, plot_diff]:
        p.getAxis("left").setPen(pg.mkPen("black"))
        p.getAxis("bottom").setPen(pg.mkPen("black"))

    # Timer to update column count every second
    timer = pg.QtCore.QTimer()
    timer.timeout.connect(update)
    timer.start(50)  # Update every .1 seconds
    app.exec()




    


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
        graph(consumer.analysers[GRAPH])
        # Start optional threads

        #consumer.start_cleanup_thread()

        return [t_save_data]

