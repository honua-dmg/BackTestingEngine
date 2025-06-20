import pandas as pd
import os
import time
import datetime as dt
import dotenv
from redis_client import r
import multiprocessing

# Load environment variables
ENVLOC = '/app/.env'
dotenv.load_dotenv(ENVLOC)
FILEPATH = os.getenv("FILEPATH")

def get_all_tick_data(date_str):
    """
    Reads all CSV files for a given date, combines them, and sorts by timestamp.
    """
    all_ticks = []
    base_path = FILEPATH
    
    for exchange in ['BSE', 'NSE']:
        date_path = os.path.join(base_path, exchange, date_str)
        if not os.path.exists(date_path):
            print(f"[SIMULATOR] Path not found: {date_path}", flush=True)
            continue
            
        for stock_file in os.listdir(date_path):
            if stock_file.endswith('.csv'):
                stock_name = stock_file.replace('.csv', '')
                try:
                    df = pd.read_csv(os.path.join(date_path, stock_file))
                    if not df.empty:
                        df['stock'] = stock_name
                        all_ticks.append(df)
                except pd.errors.EmptyDataError:
                    print(f"[SIMULATOR] Warning: {stock_file} is empty.", flush=True)
                    continue

    if not all_ticks:
        return None

    full_df = pd.concat(all_ticks)
    full_df['time'] = pd.to_datetime(full_df['time'])
    full_df = full_df.sort_values(by='time').reset_index(drop=True)
    return full_df

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

    last_tick_time = None

    for index, row in ticks_df.iterrows():
        if r.get('end') == 'true':
            print("[SIMULATOR] Simulation stopped by 'end' flag.", flush=True)
            break

        current_tick_time = row['time']
        
        # Simulate time delay
        if last_tick_time:
            delay = (current_tick_time - last_tick_time).total_seconds()
            # We cap the delay to avoid long waits for market gaps (e.g., overnight)
            if delay > 0 and delay < 5:
                time.sleep(delay)

        stock = row['stock']
        tick_data = {
            'value': row['value'],
            'volume': row['volume'],
            'time': row['time'].strftime('%Y-%m-%d %H:%M:%S')
        }
        
        r.xadd(stock, tick_data)
        r.set('time', dt.datetime.now(dt.timezone(dt.timedelta(hours=5, minutes=30))).timestamp()) # Heartbeat
        
        last_tick_time = current_tick_time

        if index > 0 and index % 10000 == 0:
            print(f"[SIMULATOR] Simulated {index}/{len(ticks_df)} ticks...", flush=True)

    print("[SIMULATOR] Market simulation finished.", flush=True)


def Simulator_worker(date_str):
    """
    The main function to start the simulator.
    """
    r.set('end','false')
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
