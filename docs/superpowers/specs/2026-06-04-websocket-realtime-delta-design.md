# Real-Time Delta Analysis via WebSocket

**Date:** 2026-06-04
**Branch:** realtime-ws (cut from master)

## Goal

Replace the CSV-based simulator with a live WebSocket feed, keeping Redis as the intermediary and preserving the `Delta_analysis` heatmap visualization from `all_in_one_v2.ipynb` cells 2 & 3.

---

## Data Flow

```
WebSocket server (ws://139.59.32.232:8765/ws)
    ↓  asyncio client in background thread
ws_producer.py  →  r.xadd(stock_key, tick_dict, maxlen=10000)
    ↓  Redis stream (key = e.g. "NSE:RELIANCE")
delta_consumer.py  →  delta_instance.parse(tick_dict)
    ↓  mutates Delta_analysis numpy arrays in place
delta_graph.py  ←  QTimer 50ms, main thread
    ↓  setImage() / setData() on 3 PyQtGraph windows
```

---

## Components

### 1. `StockAnalyser.py` — `Delta_analysis` (already present)

`Delta_analysis` is already in `StockAnalyser.py` on `master` alongside `Cumulative_Support`. No changes needed. `Algo1.transform()` returns 4 fields (`time`, `ltp`, `delta`, `ltp_type`) which `Delta_analysis.parse()` correctly unpacks.

Key attributes used by the graph:
- `instance.curr_time_idx` — number of ticks parsed so far
- `instance.base_ltp` — price offset for the numpy column index
- `instance.aggdf_buy`, `instance.aggdf_sell` — 1D arrays for active price bounds
- `instance.lowHigh['buy'|'sell']` — 2D numpy arrays (time × price)
- `instance.highLow['buy'|'sell']` — 2D numpy arrays (time × price)
- `instance.ltpdf` — 2D array of LTP values per tick

### 2. `ws_producer.py` — WebSocket → Redis

Runs `asyncio.run()` in a `threading.Thread`. Connects to the WebSocket server, sends `{"stock": stock}` to subscribe, then for each received JSON message calls `r.xadd(stock, tick_dict, maxlen=10000)`.

Exposes `start_producer(stock) -> Thread` so `delta_main.py` can start it the same way `InitialiseSimulator` is called today.

Stops when `r.get('end') == 'true'` (checked each iteration) or on websocket disconnect.

### 3. `delta_consumer.py` — Redis → Delta_analysis

Single-threaded, single-stock consumer. Uses one `xgroup_create` + `xreadgroup` loop (same pattern as `Consumers.py`). On each tick calls `delta_instance.parse(tick_dict)`. Runs until `r.get('end') == 'true'`.

Exposes `start_consumer(stock, delta_instance) -> Thread`.

No rebalancing, no multi-stock assignment, no monitoring thread — this is a focused single-stock live viewer.

### 4. `delta_graph.py` — Live PyQtGraph visualization

Extracted from cell 3 of `all_in_one_v2.ipynb` into `delta_graph(instance)`.

- Sets up 3 windows once on entry: **combined**, **components** (4 heatmaps), **stats** (max/mean/min + d1/MAD signal)
- QTimer fires every 50ms, calling `update()` which:
  1. Reads `rows_used = int(instance.curr_time_idx)`
  2. Computes active price bounds from `aggdf_buy`/`aggdf_sell`
  3. Re-slices `lowHigh`/`highLow` arrays and calls `setImage()` on each `ImageItem`
  4. Updates LTP `PlotCurveItem` via `setData()`
- Blocks on `app.exec()` — must be called from the main thread

Waits for `instance.curr_time_idx > 0` before initializing windows (same pattern as `graph.py`'s `while instance.ltpDf.empty: sleep(1)`).

### 5. `delta_main.py` — Entry point

```
r.flushall()
instance = Delta_analysis()
start_producer(stock)   # background thread
start_consumer(stock, instance)  # background thread
delta_graph(instance)   # blocks main thread on Qt event loop
r.set('end', 'true')    # signal threads to stop on Qt window close
```

Stock is read from `config.py` (`STOCKS[0]`, `EXCHANGE`) exactly as today.

---

## Tick Format

WebSocket messages are JSON dicts with this shape (already compatible with `Delta_analysis.parse()`):

```json
{
  "timestamp": "15:30:01",
  "stonk": "NSE:RELIANCE",
  "last_price": "2345.0",
  "last_traded_quantity": "10",
  "total_buy_quantity": "405665",
  "total_sell_quantity": "603460",
  "buy_price_1": "...", "buy_qty_1": "...",
  ...
}
```

No translation step needed — the dict is passed directly to `r.xadd` and then to `parse()`.

---

## Threading Notes

- `Delta_analysis` numpy arrays are mutated by the consumer thread and read by the Qt main thread. This is safe for display purposes (array reads are non-destructive and the worst case is a single stale frame).
- No explicit locking is added — consistent with how `graph.py` works with `Cumulative_Support` today.

---

## Out of Scope

- Multi-stock support
- Reconnect / retry logic for the WebSocket
- Persistence of parsed data to CSV
