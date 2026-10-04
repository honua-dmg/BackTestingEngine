# Real-Time WebSocket Delta Analysis — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the CSV simulator with a live WebSocket feed, routing ticks through Redis into the existing `Delta_analysis` and rendering the 3-window heatmap visualization live.

**Architecture:** WebSocket client (asyncio thread) pushes ticks to a Redis stream; a consumer thread reads the stream and calls `Delta_analysis.parse()`; PyQtGraph polls the shared numpy arrays via QTimer on the main thread.

**Tech Stack:** Python 3, `websockets`, `redis-py`, `pyqtgraph`, `PyQt5`, `numpy`

**Note:** `Delta_analysis` already exists in `app/StockAnalyser.py` on `master`. `Algo1.transform()` returns `{'time', 'ltp', 'delta', 'ltp_type'}` — 4 fields, correctly unpacked by `parse()`. No changes to `StockAnalyser.py` needed.

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `app/ws_producer.py` | Create | Async WebSocket client → `r.xadd` |
| `app/delta_consumer.py` | Create | Redis `xreadgroup` loop → `Delta_analysis.parse()` |
| `app/delta_graph.py` | Create | Live PyQtGraph 3-window visualization with QTimer |
| `app/delta_main.py` | Create | Entry point — wires all threads + Qt main loop |
| `app/tests/__init__.py` | Create | Make tests a package |
| `app/tests/test_delta_analysis.py` | Create | Smoke tests verifying existing `Delta_analysis` |
| `app/tests/test_ws_producer.py` | Create | Unit tests for `ws_producer` |
| `app/tests/test_delta_consumer.py` | Create | Unit tests for `delta_consumer` |

---

## Task 1: Smoke-test the existing `Delta_analysis`

Verify the existing implementation parses ticks correctly before building on top of it. No code changes to `StockAnalyser.py`.

**Files:**
- Create: `app/tests/__init__.py`
- Create: `app/tests/test_delta_analysis.py`

- [ ] **Step 1: Create the tests package**

```bash
mkdir -p app/tests && touch app/tests/__init__.py
```

- [ ] **Step 2: Write the tests**

Create `app/tests/test_delta_analysis.py`:

```python
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np
from StockAnalyser import Delta_analysis

TICK = {
    'timestamp': '09:15:17', 'stonk': 'NSE:TATAMOTORS',
    'last_price': '710.0', 'last_traded_quantity': '5',
    'average_traded_price': '710.0', 'volume_traded': '100',
    'total_buy_quantity': '283158', 'total_sell_quantity': '542056',
    'open': '710.0', 'high': '710.0', 'low': '710.0', 'close': '709.0',
    'change': '0.1',
    'buy_price_1': '710.05', 'buy_qty_1': '100', 'buy_orders_1': '1',
    'sell_price_1': '711.0',  'sell_qty_1': '100', 'sell_orders_1': '1',
    'buy_price_2': '709.9',  'buy_qty_2': '50',  'buy_orders_2': '1',
    'sell_price_2': '711.5',  'sell_qty_2': '50',  'sell_orders_2': '1',
    'buy_price_3': '709.8',  'buy_qty_3': '30',  'buy_orders_3': '1',
    'sell_price_3': '712.0',  'sell_qty_3': '30',  'sell_orders_3': '1',
    'buy_price_4': '709.7',  'buy_qty_4': '20',  'buy_orders_4': '1',
    'sell_price_4': '712.5',  'sell_qty_4': '20',  'sell_orders_4': '1',
    'buy_price_5': '709.6',  'buy_qty_5': '10',  'buy_orders_5': '1',
    'sell_price_5': '713.0',  'sell_qty_5': '10',  'sell_orders_5': '1',
}

TICK2 = {**TICK, 'volume_traded': '200'}


def test_parse_increments_curr_time_idx():
    inst = Delta_analysis()
    inst.parse(TICK)
    assert inst.curr_time_idx == 1


def test_parse_sets_base_ltp_on_first_tick():
    inst = Delta_analysis()
    inst.parse(TICK)
    assert inst.base_ltp is not None


def test_parse_zero_delta_is_noop():
    """Same volume_traded twice → delta_vol == 0 → parse returns early."""
    inst = Delta_analysis()
    inst.parse(TICK)
    inst.parse(TICK)
    assert inst.curr_time_idx == 1


def test_parse_accumulates_aggdf():
    inst = Delta_analysis()
    inst.parse(TICK)
    ltp_idx = int(float(TICK['last_price'])) - inst.base_ltp
    assert inst.aggdf_buy[ltp_idx] + inst.aggdf_sell[ltp_idx] > 0


def test_parse_populates_lowhigh_row():
    inst = Delta_analysis()
    inst.parse(TICK)
    inst.parse(TICK2)
    assert not np.all(np.isnan(inst.lowHigh['buy'][0]))


def test_price_bounds_expand_left():
    inst = Delta_analysis()
    inst.parse(TICK)
    original_base = inst.base_ltp
    low_tick = {**TICK2, 'last_price': str(original_base - 100), 'volume_traded': '300'}
    inst.parse(low_tick)
    assert inst.base_ltp < original_base


def test_price_bounds_expand_right():
    inst = Delta_analysis()
    inst.parse(TICK)
    original_width = inst.WIDTH
    high_tick = {**TICK2, 'last_price': str(int(float(TICK['last_price'])) + inst.WIDTH + 10), 'volume_traded': '300'}
    inst.parse(high_tick)
    assert inst.WIDTH > original_width
```

- [ ] **Step 3: Run tests — expect all 7 to pass**

```bash
cd app && python -m pytest tests/test_delta_analysis.py -v
```

Expected output: 7 tests PASS. If any fail, the existing `Delta_analysis` has a bug that must be fixed before proceeding.

- [ ] **Step 4: Commit**

```bash
git add app/tests/__init__.py app/tests/test_delta_analysis.py
git commit -m "add smoke tests for existing Delta_analysis"
```

---

## Task 2: `ws_producer.py` — WebSocket → Redis

**Files:**
- Create: `app/ws_producer.py`
- Create: `app/tests/test_ws_producer.py`

- [ ] **Step 1: Write the failing test**

Create `app/tests/test_ws_producer.py`:

```python
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import asyncio
import json
import threading
from unittest.mock import MagicMock, patch

TICK = json.dumps({
    'timestamp': '09:15:17', 'stonk': 'NSE:RELIANCE',
    'last_price': '2345.0', 'volume_traded': '100',
})


def _fake_ws_connect(messages):
    async def fake_connect(url):
        class FakeWS:
            def __aiter__(self): return self
            _idx = 0
            async def __anext__(self):
                if self._idx >= len(messages):
                    raise StopAsyncIteration
                msg = messages[self._idx]
                self._idx += 1
                return msg
            async def send(self, data): pass
            async def __aenter__(self): return self
            async def __aexit__(self, *a): pass
        return FakeWS()
    return fake_connect


def test_producer_pushes_each_message_to_redis():
    mock_r = MagicMock()
    mock_r.get.return_value = 'false'

    with patch('ws_producer.websockets.connect', new=_fake_ws_connect([TICK, TICK])), \
         patch('ws_producer.r', mock_r):
        from ws_producer import _run_async_producer
        asyncio.run(_run_async_producer('NSE:RELIANCE'))

    assert mock_r.xadd.call_count == 2
    stream_key = mock_r.xadd.call_args_list[0][0][0]
    assert stream_key == 'NSE:RELIANCE'


def test_producer_stops_on_end_flag():
    mock_r = MagicMock()
    call_count = [0]

    def fake_get(key):
        call_count[0] += 1
        return 'true' if call_count[0] > 1 else 'false'

    mock_r.get.side_effect = fake_get

    with patch('ws_producer.websockets.connect', new=_fake_ws_connect([TICK, TICK, TICK])), \
         patch('ws_producer.r', mock_r):
        from ws_producer import _run_async_producer
        asyncio.run(_run_async_producer('NSE:RELIANCE'))

    assert mock_r.xadd.call_count <= 1


def test_start_producer_returns_thread():
    mock_r = MagicMock()
    mock_r.get.return_value = 'false'

    with patch('ws_producer.websockets.connect', new=_fake_ws_connect([])), \
         patch('ws_producer.r', mock_r):
        from ws_producer import start_producer
        t = start_producer('NSE:RELIANCE')
        t.join(timeout=3)
        assert isinstance(t, threading.Thread)
```

- [ ] **Step 2: Run test — expect ImportError**

```bash
cd app && python -m pytest tests/test_ws_producer.py -v
```

Expected: `ModuleNotFoundError: No module named 'ws_producer'`

- [ ] **Step 3: Create `app/ws_producer.py`**

```python
import asyncio
import json
import threading
import websockets
from config import r, STOCKS, EXCHANGE

HOST = "ws://139.59.32.232:8765/ws"


async def _run_async_producer(stock: str):
    async with websockets.connect(HOST) as ws:
        await ws.send(json.dumps({"stock": stock}))
        async for message in ws:
            if r.get('end') == 'true':
                break
            tick = json.loads(message)
            r.xadd(stock, tick, maxlen=10000)


def start_producer(stock: str) -> threading.Thread:
    def _run():
        asyncio.run(_run_async_producer(stock))

    t = threading.Thread(target=_run, name="WsProducer", daemon=True)
    t.start()
    return t


if __name__ == "__main__":
    stock = f"{EXCHANGE}:{STOCKS[0]}"
    start_producer(stock).join()
```

- [ ] **Step 4: Run tests — expect all 3 pass**

```bash
cd app && python -m pytest tests/test_ws_producer.py -v
```

Expected: 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/ws_producer.py app/tests/test_ws_producer.py
git commit -m "add ws_producer: async websocket client that pushes ticks to Redis stream"
```

---

## Task 3: `delta_consumer.py` — Redis stream → `Delta_analysis.parse()`

**Files:**
- Create: `app/delta_consumer.py`
- Create: `app/tests/test_delta_consumer.py`

- [ ] **Step 1: Write the failing test**

Create `app/tests/test_delta_consumer.py`:

```python
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import threading
from unittest.mock import MagicMock, patch
from StockAnalyser import Delta_analysis

TICK = {
    'timestamp': '09:15:17', 'stonk': 'NSE:RELIANCE',
    'last_price': '710.0', 'last_traded_quantity': '5',
    'average_traded_price': '710.0', 'volume_traded': '100',
    'total_buy_quantity': '100', 'total_sell_quantity': '200',
    'open': '710.0', 'high': '710.0', 'low': '710.0', 'close': '709.0',
    'change': '0.1',
    'buy_price_1': '710.05', 'buy_qty_1': '100', 'buy_orders_1': '1',
    'sell_price_1': '711.0',  'sell_qty_1': '100', 'sell_orders_1': '1',
    'buy_price_2': '709.9',  'buy_qty_2': '50',  'buy_orders_2': '1',
    'sell_price_2': '711.5',  'sell_qty_2': '50',  'sell_orders_2': '1',
    'buy_price_3': '709.8',  'buy_qty_3': '30',  'buy_orders_3': '1',
    'sell_price_3': '712.0',  'sell_qty_3': '30',  'sell_orders_3': '1',
    'buy_price_4': '709.7',  'buy_qty_4': '20',  'buy_orders_4': '1',
    'sell_price_4': '712.5',  'sell_qty_4': '20',  'sell_orders_4': '1',
    'buy_price_5': '709.6',  'buy_qty_5': '10',  'buy_orders_5': '1',
    'sell_price_5': '713.0',  'sell_qty_5': '10',  'sell_orders_5': '1',
}


def test_consumer_calls_parse_for_each_claimed_message():
    mock_r = MagicMock()
    mock_r.xautoclaim.return_value = (None, [('id1', TICK), ('id2', TICK)], None)
    mock_r.xreadgroup.return_value = None
    calls = [0]

    def fake_get(key):
        calls[0] += 1
        return 'true' if calls[0] > 2 else 'false'

    mock_r.get.side_effect = fake_get
    instance = MagicMock(spec=Delta_analysis)

    with patch('delta_consumer.r', mock_r):
        from delta_consumer import start_consumer
        t = start_consumer('NSE:RELIANCE', instance)
        t.join(timeout=3)

    assert instance.parse.call_count >= 2


def test_start_consumer_returns_thread():
    mock_r = MagicMock()
    mock_r.xautoclaim.return_value = (None, [], None)
    mock_r.xreadgroup.return_value = None
    mock_r.get.return_value = 'true'
    instance = MagicMock(spec=Delta_analysis)

    with patch('delta_consumer.r', mock_r):
        from delta_consumer import start_consumer
        t = start_consumer('NSE:RELIANCE', instance)
        t.join(timeout=3)
        assert isinstance(t, threading.Thread)
```

- [ ] **Step 2: Run test — expect ImportError**

```bash
cd app && python -m pytest tests/test_delta_consumer.py -v
```

Expected: `ModuleNotFoundError: No module named 'delta_consumer'`

- [ ] **Step 3: Create `app/delta_consumer.py`**

```python
import threading
import logging
from config import r


def _consumer_loop(stock: str, instance) -> None:
    group = stock
    consumer = stock
    try:
        r.xgroup_create(name=stock, groupname=group, mkstream=True, id='0')
    except Exception:
        pass  # group already exists

    logging.info(f"[CONSUMER] starting for {stock}")
    while r.get('end') != 'true':
        _, claimed, _ = r.xautoclaim(stock, group, consumer,
                                     min_idle_time=0, start_id='0-0')
        for msg_id, tick in claimed:
            instance.parse(tick)
            r.xack(stock, group, msg_id)

        new = r.xreadgroup(groupname=group, consumername=consumer,
                           streams={stock: '>'}, block=10)
        if new:
            for _, messages in new:
                for msg_id, tick in messages:
                    try:
                        instance.parse(tick)
                        r.xack(stock, group, msg_id)
                    except Exception as e:
                        logging.error(f"[CONSUMER] tick error: {e}")

    logging.info(f"[CONSUMER] stopped for {stock}")


def start_consumer(stock: str, instance) -> threading.Thread:
    t = threading.Thread(target=_consumer_loop, args=(stock, instance),
                         name="DeltaConsumer", daemon=True)
    t.start()
    return t
```

- [ ] **Step 4: Run tests — expect both pass**

```bash
cd app && python -m pytest tests/test_delta_consumer.py -v
```

Expected: 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/delta_consumer.py app/tests/test_delta_consumer.py
git commit -m "add delta_consumer: single-stock Redis stream consumer for Delta_analysis"
```

---

## Task 4: `delta_graph.py` — Live PyQtGraph visualization

The 3-window setup from `all_in_one_v2.ipynb` cell 3, extracted into a reusable function. QTimer re-renders every 50ms by re-slicing the live numpy arrays.

**Files:**
- Create: `app/delta_graph.py`
- Create: `app/tests/test_delta_graph.py`

- [ ] **Step 1: Write the smoke test**

Create `app/tests/test_delta_graph.py`:

```python
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def test_delta_graph_importable():
    import delta_graph  # noqa: F401


def test_delta_graph_exposes_function():
    import delta_graph
    assert callable(delta_graph.delta_graph)
```

- [ ] **Step 2: Run test — expect ImportError**

```bash
cd app && python -m pytest tests/test_delta_graph.py -v
```

Expected: `ModuleNotFoundError: No module named 'delta_graph'`

- [ ] **Step 3: Create `app/delta_graph.py`**

```python
import time
import numpy as np
import pyqtgraph as pg
from PyQt5.QtCore import QRectF, Qt


def _active_price_bounds(aggdf_buy, aggdf_sell):
    active = np.where((aggdf_buy > 0) & (aggdf_sell > 0))[0]
    if active.size == 0:
        return 0, max(0, len(aggdf_buy) - 1)
    return int(active[0]), int(active[-1])


def _make_component_map(arr, true_code):
    out = np.zeros_like(arr, dtype=np.uint8)
    valid = ~np.isnan(arr)
    out[valid] = np.where(arr[valid] >= 0, true_code, 0).astype(np.uint8)
    out[~valid] = 16
    return out


def _rolling_mad(values, window=20):
    values = np.asarray(values, dtype=float)
    out = np.full_like(values, np.nan)
    for i in range(window - 1, values.size):
        chunk = values[i - window + 1: i + 1]
        chunk = chunk[np.isfinite(chunk)]
        if chunk.size:
            out[i] = np.median(np.abs(chunk - np.median(chunk)))
    return out


def _first_order_mad_signal(series, window=20, eps=1e-12):
    series = np.asarray(series, dtype=float)
    delta = np.diff(series, prepend=np.nan)
    mad = _rolling_mad(delta, window)
    mad = np.where(mad > eps, mad, np.nan)
    return delta / mad


BASE_LUT = np.array([
    [255,   0,   0, 170],   # 0  Red
    [  0, 114, 178, 170],   # 1  Blue
    [230, 159,   0, 170],   # 2  Orange
    [ 86, 180, 233, 170],   # 3  Sky Blue
    [  0, 158, 115, 170],   # 4  Bluish Green
    [204, 121, 167, 170],   # 5  Purple
    [240, 228,  66, 170],   # 6  Yellow
    [  0,   0, 128, 170],   # 7  Navy
    [255, 105, 180, 170],   # 8  Pink
    [ 27,  94,  32, 170],   # 9  Dark Green
    [139,  69,  19, 170],   # 10 Brown
    [128,   0,   0, 170],   # 11 Maroon
    [  0, 128, 128, 170],   # 12 Teal
    [245, 245, 245, 170],   # 13 Off-White
    [119, 119, 119, 170],   # 14 Gray
    [ 57, 255,  20, 170],   # 15 Neon Green
    [  0,   0,   0,   0],   # 16 Transparent
], dtype=np.ubyte)

FULL_LUT = np.zeros((256, 4), dtype=np.ubyte)
FULL_LUT[:17] = BASE_LUT


def delta_graph(instance):
    """
    Launch the live 3-window PyQtGraph visualization.
    Blocks on app.exec() — must be called from the main thread.
    """
    while instance.curr_time_idx == 0:
        time.sleep(0.05)

    app = pg.mkQApp("Delta Analysis — Live")

    win_combined   = pg.GraphicsLayoutWidget(title="Combined")
    win_components = pg.GraphicsLayoutWidget(title="Components")
    win_stats      = pg.GraphicsLayoutWidget(title="Stats")
    for w in (win_combined, win_components, win_stats):
        w.setBackground("white")

    p_combined = win_combined.addPlot(title="Combined State (0..15)")
    p_combined.showGrid(x=True, y=True, alpha=0.15)
    p_combined.setLabel("left", "LTP")
    p_combined.setLabel("bottom", "Tick Index")

    img_combined = pg.ImageItem()
    img_combined.setLookupTable(FULL_LUT)
    p_combined.addItem(img_combined)
    ltp_line_combined = p_combined.plot([], [], pen=pg.mkPen(color=(0, 0, 255), width=2))

    # (label, arr_key, side, heatmap_bit_code)
    component_specs = [
        ("hl_buy",  'highLow', 'buy',  8),
        ("lh_buy",  'lowHigh', 'buy',  4),
        ("hl_sell", 'highLow', 'sell', 2),
        ("lh_sell", 'lowHigh', 'sell', 1),
    ]

    comp_plots, comp_imgs, comp_ltps = [], [], []
    stat_plots, signal_plots = [], []

    for i, (label, arr_key, side, code) in enumerate(component_specs):
        row, col = divmod(i, 2)

        p = win_components.addPlot(row=row, col=col, title=f"{label} (0/{code}/T)")
        p.showGrid(x=True, y=True, alpha=0.15)
        p.setLabel("left", "LTP")
        p.setLabel("bottom", "Tick Index")
        img = pg.ImageItem()
        img.setLookupTable(FULL_LUT)
        p.addItem(img)
        ltp_c = p.plot([], [], pen=pg.mkPen(color=(0, 0, 255), width=2))
        comp_plots.append(p)
        comp_imgs.append(img)
        comp_ltps.append(ltp_c)

        sp = win_stats.addPlot(row=row * 2, col=col, title=f"{label} Max/Mean/Min")
        sp.showGrid(x=True, y=True, alpha=0.15)
        sp.setLabel("left", "Value")
        sp.setLabel("bottom", "Tick Index")
        sp_max  = sp.plot([], [], pen=pg.mkPen(color=(0, 0, 255),  width=2), name="Max")
        sp_mean = sp.plot([], [], pen=pg.mkPen(color=(0, 200, 0),   width=2), name="Mean")
        sp_min  = sp.plot([], [], pen=pg.mkPen(color=(255, 200, 0), width=2), name="Min")
        stat_plots.append((sp, sp_max, sp_mean, sp_min))

        sp_sig = win_stats.addPlot(row=row * 2 + 1, col=col, title=f"{label} d1/MAD")
        sp_sig.showGrid(x=True, y=True, alpha=0.15)
        sp_sig.setLabel("left", "Signal")
        sp_sig.setLabel("bottom", "Tick Index")
        sig_mean_line = sp_sig.plot([], [], pen=pg.mkPen(color=(0, 200, 0),   width=2), name="Mean Signal")
        sig_min_line  = sp_sig.plot([], [], pen=pg.mkPen(color=(255, 200, 0), width=2), name="Min Signal")
        sp_sig.addLine(y=-3.5, pen=pg.mkPen(color=(120, 120, 120), width=1, style=Qt.DashLine))
        sp_sig.addLine(y=3.5,  pen=pg.mkPen(color=(120, 120, 120), width=1, style=Qt.DashLine))
        signal_plots.append((sp_sig, sig_mean_line, sig_min_line))

    master = comp_plots[0]
    for p in comp_plots[1:]:
        p.setXLink(master)
        p.setYLink(master)

    for w in (win_combined, win_components, win_stats):
        w.show()

    def update():
        rows_used = int(instance.curr_time_idx)
        if rows_used < 1:
            return

        min_idx, max_idx = _active_price_bounds(instance.aggdf_buy, instance.aggdf_sell)
        col_slice = slice(min_idx, max_idx + 1)

        lh_buy  = instance.lowHigh['buy'][:rows_used,  col_slice]
        hl_buy  = instance.highLow['buy'][:rows_used,  col_slice]
        lh_sell = instance.lowHigh['sell'][:rows_used, col_slice]
        hl_sell = instance.highLow['sell'][:rows_used, col_slice]

        valid_buy  = ~np.isnan(lh_buy)
        valid_sell = ~np.isnan(lh_sell)

        buy_code  = np.zeros_like(lh_buy,  dtype=np.uint8)
        sell_code = np.zeros_like(lh_sell, dtype=np.uint8)
        buy_code[valid_buy]   = (lh_buy[valid_buy]   >= 0).astype(np.uint8) + 2 * (hl_buy[valid_buy]   >= 0).astype(np.uint8)
        sell_code[valid_sell] = (lh_sell[valid_sell] >= 0).astype(np.uint8) + 2 * (hl_sell[valid_sell] >= 0).astype(np.uint8)

        heatmap = sell_code + 4 * buy_code
        heatmap[~(valid_buy & valid_sell)] = 16

        min_ltp = float(instance.base_ltp + min_idx)
        max_ltp = float(instance.base_ltp + max_idx)
        rect = QRectF(0, min_ltp, rows_used, max(1.0, max_ltp - min_ltp + 1.0))

        img_combined.setImage(heatmap, autoLevels=False)
        img_combined.setRect(rect)

        ltp_vals  = instance.ltpdf[:rows_used, 0].astype(float)
        valid_ltp = np.isfinite(ltp_vals) & (ltp_vals != 0)
        x_ltp = np.arange(rows_used, dtype=float)[valid_ltp]
        y_ltp = ltp_vals[valid_ltp]
        ltp_line_combined.setData(x_ltp, y_ltp)
        p_combined.setYRange(min_ltp, max_ltp, padding=0)

        arrays = [hl_buy, lh_buy, hl_sell, lh_sell]
        codes  = [8, 4, 2, 1]
        for i, (arr, code) in enumerate(zip(arrays, codes)):
            comp_imgs[i].setImage(_make_component_map(arr, code), autoLevels=False)
            comp_imgs[i].setRect(rect)
            comp_ltps[i].setData(x_ltp, y_ltp)

            arr_f = arr.astype(float)
            with np.errstate(all='ignore'):
                row_max  = np.nanmax(arr_f,  axis=1)
                row_mean = np.nanmean(arr_f, axis=1)
                row_min  = np.nanmin(arr_f,  axis=1)

            valid = np.isfinite(row_max) | np.isfinite(row_mean) | np.isfinite(row_min)
            if np.any(valid):
                xv = np.arange(rows_used, dtype=float)[valid]
                _, sp_max, sp_mean, sp_min = stat_plots[i]
                sp_max.setData(xv,  row_max[valid])
                sp_mean.setData(xv, row_mean[valid])
                sp_min.setData(xv,  row_min[valid])

                _, sig_mean_line, sig_min_line = signal_plots[i]
                sig_m = _first_order_mad_signal(row_mean)
                sig_n = _first_order_mad_signal(row_min)
                vs = np.isfinite(sig_m) | np.isfinite(sig_n)
                if np.any(vs):
                    xs = np.arange(rows_used, dtype=float)[vs]
                    sig_mean_line.setData(xs, sig_m[vs])
                    sig_min_line.setData(xs,  sig_n[vs])

    timer = pg.QtCore.QTimer()
    timer.timeout.connect(update)
    timer.start(50)
    app.exec()
```

- [ ] **Step 4: Run smoke tests — expect both pass**

```bash
cd app && python -m pytest tests/test_delta_graph.py -v
```

Expected: 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/delta_graph.py app/tests/test_delta_graph.py
git commit -m "add delta_graph: live PyQtGraph 3-window visualization with QTimer"
```

---

## Task 5: `delta_main.py` — Entry point

**Files:**
- Create: `app/delta_main.py`

- [ ] **Step 1: Create `app/delta_main.py`**

```python
import logging
from config import r, STOCKS, EXCHANGE
from StockAnalyser import Delta_analysis
from ws_producer import start_producer
from delta_consumer import start_consumer
from delta_graph import delta_graph


def run():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(threadName)s] %(message)s',
    )

    stock = f"{EXCHANGE}:{STOCKS[0]}"
    logging.info(f"[MAIN] starting live delta analysis for {stock}")

    r.flushall()
    r.set('end', 'false')

    instance = Delta_analysis()
    producer_thread = start_producer(stock)
    consumer_thread = start_consumer(stock, instance)

    try:
        delta_graph(instance)  # blocks on Qt event loop
    except KeyboardInterrupt:
        logging.info("[MAIN] interrupted")
    finally:
        r.set('end', 'true')
        producer_thread.join(timeout=5)
        consumer_thread.join(timeout=5)
        logging.info("[MAIN] exited cleanly")


if __name__ == "__main__":
    run()
```

- [ ] **Step 2: Verify imports resolve**

```bash
cd app && python -c "import delta_main; print('imports ok')"
```

Expected: `imports ok`

- [ ] **Step 3: Run the full test suite**

```bash
cd app && python -m pytest tests/ -v
```

Expected: all 12 tests PASS

- [ ] **Step 4: Commit**

```bash
git add app/delta_main.py
git commit -m "add delta_main: entry point wiring websocket producer, consumer, and live graph"
```

---

## Task 6: Manual smoke test

- [ ] **Step 1: Confirm Redis is running**

```bash
redis-cli ping
```

Expected: `PONG`

- [ ] **Step 2: Confirm the WebSocket server is reachable**

```bash
cd app && python -c "
import asyncio, websockets, json
async def check():
    async with websockets.connect('ws://139.59.32.232:8765/ws') as ws:
        await ws.send(json.dumps({'stock': 'NSE:RELIANCE'}))
        msg = await asyncio.wait_for(ws.recv(), timeout=5)
        print('Got tick keys:', list(json.loads(msg).keys())[:5])
asyncio.run(check())
"
```

Expected: prints first 5 field names of a live tick.

- [ ] **Step 3: Run the live system**

```bash
cd app && python delta_main.py
```

Expected: three PyQtGraph windows open; heatmaps start populating within a few seconds.

- [ ] **Step 4: Verify ticks are flowing through Redis**

While windows are open, run in a separate terminal:

```bash
redis-cli xlen NSE:RELIANCE
```

Expected: number increases each time you run it.

- [ ] **Step 5: Confirm clean shutdown**

Close all three windows. Expected final log line: `[MAIN] exited cleanly`
