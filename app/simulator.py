import pandas as pd
import os
import time
import datetime as dt
import json
from config import r,STOCKS,CONFIG_DIR,TEST,FILEPATH,EXCHANGE,s3
import multiprocessing
import logging





def tokenStockMapping(exchange):
    """
    Maps tokens to their corresponding stock symbols.
    
    Args:
        exchange (str): The exchange name ('NSE' or 'BSE').
    
    Returns:
        dict: A dictionary mapping tokens to their stock symbols.
    """
    df = pd.read_csv(os.path.join(CONFIG_DIR, f"{exchange}.csv"))
    return dict(zip( df['instrument_token'],df['tradingsymbol']))

def get_all_tick_data(date_str):
    """
    Reads all CSV files for a given date, combines them, and sorts by timestamp.
    """

    stocks  = json.load(open(os.path.join(CONFIG_DIR, "stocks.json")))[TEST]
    nse = tokenStockMapping('NSE')
    bse = tokenStockMapping('BSE')
    tickdata = pd.DataFrame()
    base_path = FILEPATH
    
    for stock in STOCKS:
        nse_data = pd.DataFrame()
        bse_data = pd.DataFrame()
        if "NSE" in stocks[stock]:
            nse_path = os.path.join(base_path, "NSE",stock, f"{date_str}.csv")
            nse_data = pd.read_csv(nse_path)
            nse_data['stonk'] = nse_data['stonk'].apply(lambda x: f"NSE:{nse[x]}")
        if "BSE" in stocks[stock]:
            bse_path = os.path.join(base_path, "BSE",stock, f"{date_str}.csv")
            bse_data = pd.read_csv(bse_path)
            bse_data['stonk'] = bse_data['stonk'].apply(lambda x: f"BSE:{bse[x]}")
        try:
            df = pd.concat([nse_data,bse_data],ignore_index=True)

            tickdata = pd.concat([tickdata,df],ignore_index=True)
        except Exception as e:
            print(f"[SIMULATOR] Error reading {stock} data: {e}")
            continue
        logging.info(f"[SIMULATOR] Read {stock} data")

    if tickdata.empty:
        return None

    #tickdata['timestamp'] = pd.to_datetime(tickdata['timestamp'],format='%H:%M:%S')
    tickdata = tickdata.sort_values(by=['timestamp','volume_traded']).reset_index(drop=True)
    print("[INFO] loaded data into memory for simulation")
    return tickdata

def run_simulation(date_str):
    """
    Runs the market simulation for a given date.
    """
    print(f"[SIMULATOR] Starting market simulation for date: {date_str}", flush=True)

    ticks_df = get_all_tick_data(date_str)
    
    if ticks_df is None or ticks_df.empty:
        print(f"[SIMULATOR] No data found for {date_str}. Exiting simulation.", flush=True)
        return

    print(f"[SIMULATOR] Loaded {len(ticks_df)} ticks for simulation.", flush=True)
    r.set('end','false')
    for index, row in ticks_df.iterrows():
        if r.get('end') == 'true':
            print("[SIMULATOR] Simulation stopped by 'end' flag.", flush=True)
            break

        stock = row['stonk']
        tick_data = row.to_dict()
        time.sleep(.001)
        r.xadd(stock.split(':')[1], tick_data,maxlen=10000)
        
        if index > 0 and index % 10000 == 0:
            print(f"[SIMULATOR] Simulated {index}/{len(ticks_df)} ticks...", flush=True)

    print("[SIMULATOR] Market simulation finished.", flush=True)
    


def Simulator_worker(date_str):
    """
    The main function to start the simulator.
    """
    
    try:
        print('[SIMULATOR] starting simulation')
        run_simulation(date_str)
    except Exception as e:
        print(e)


def InitialiseSimulator(date_str):
    """
    Initializes the simulator process.
    """
    p = multiprocessing.Process(target=Simulator_worker, args=(date_str,))
    p.start()
    return p
