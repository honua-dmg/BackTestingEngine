"""
Delta analysis visualisation, shared by the live pipeline and the notebook.

build_windows() constructs the Qt scene; render() computes one frame's worth of
data and pushes it into that scene. delta_graph() drives render() on a timer for
the live feed; show_static() calls it once over an already-parsed history.
"""
import time
import numpy as np
import pyqtgraph as pg
from pyqtgraph.Qt import QtCore, QtWidgets
QRectF = QtCore.QRectF

# A "drop" is a tick-to-tick fall in the count>0 of more than this many cells.
COUNT_DROP_THRESHOLD = 10
# A "spike" is |d1 of max| exceeding this many points (cross markers).
SPIKE_THRESHOLD = 5.0
# One colour per signal: buys green shades, sells red shades.
SPIKE_COLORS = [
    (  0, 150,   0),   # hl_buy  - dark green
    (100, 220, 100),   # lh_buy  - light green
    (150,   0,   0),   # hl_sell - dark red
    (240, 100, 100),   # lh_sell - light red
]

# (label, arr_key, side, heatmap_bit_code)
COMPONENT_SPECS = [
    ("hl_buy",  'highLow', 'buy',  8),
    ("lh_buy",  'lowHigh', 'buy',  4),
    ("hl_sell", 'highLow', 'sell', 2),
    ("lh_sell", 'lowHigh', 'sell', 1),
]


def _active_price_bounds(aggdf_buy, aggdf_sell):
    active = np.where((aggdf_buy > 0) | (aggdf_sell > 0))[0]
    if active.size == 0:
        return 0, max(0, len(aggdf_buy) - 1)
    pad = 10
    lo = max(0, int(active[0]) - pad)
    hi = min(len(aggdf_buy) - 1, int(active[-1]) + pad)
    return lo, hi


def _make_component_map(arr, true_code):
    out = np.zeros_like(arr, dtype=np.uint8)
    valid = ~np.isnan(arr)
    out[valid] = np.where(arr[valid] >= 0, true_code, 0).astype(np.uint8)
    out[~valid] = 16
    return out


def _first_order_diff(series):
    """Difference between the current and immediate previous value."""
    series = np.asarray(series, dtype=float)
    return np.diff(series, prepend=np.nan)


def _cumulative_pos_pct(arr):
    """Per-row cumulative (tick 0..row) % of valid cells that are >=0."""
    # ponytail: same math as backtest.export_mark_stats.cumulative_pos_neg_pct,
    # duplicated because importing that module pulls in config/S3 credentials.
    valid = ~np.isnan(arr)
    cum_valid = np.cumsum(valid.sum(axis=1).astype(float))
    cum_pos = np.cumsum((valid & (arr >= 0)).sum(axis=1).astype(float))
    pct = np.full(cum_valid.shape, np.nan)
    has_data = cum_valid > 0
    pct[has_data] = 100.0 * cum_pos[has_data] / cum_valid[has_data]
    return pct


def _active_bounds_series(arr, min_idx, base_ltp):
    """Per-row min/max active LTP lines for a component array."""
    price_axis = base_ltp + np.arange(min_idx, min_idx + arr.shape[1], dtype=float)
    active = (~np.isnan(arr)) & (arr >= 0)

    min_series = np.full(arr.shape[0], np.nan, dtype=float)
    max_series = np.full(arr.shape[0], np.nan, dtype=float)

    valid_rows = np.any(active, axis=1)
    if np.any(valid_rows):
        per_row_prices = np.where(active, price_axis[np.newaxis, :], np.nan)
        min_series[valid_rows] = np.nanmin(per_row_prices[valid_rows], axis=1)
        max_series[valid_rows] = np.nanmax(per_row_prices[valid_rows], axis=1)

    return min_series, max_series


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


def build_windows(sync_ranges=False):
    """
    Build the four-window Qt scene and return handles to everything render()
    writes into. sync_ranges links pan/zoom across all windows (the notebook's
    behaviour); the live graph leaves it off and only links the component plots.
    """
    app = pg.mkQApp("Delta Analysis")

    win_combined   = pg.GraphicsLayoutWidget(title="Combined")
    win_components = pg.GraphicsLayoutWidget(title="Components")
    win_stats      = pg.GraphicsLayoutWidget(title="Stats")
    win_pct        = pg.GraphicsLayoutWidget(title="Pos%")
    for w in (win_combined, win_components, win_stats, win_pct):
        w.setBackground("white")

    p_combined = win_combined.addPlot(title="Combined State (0..15)")
    p_combined.showGrid(x=True, y=True, alpha=0.15)
    p_combined.setLabel("left", "LTP")
    p_combined.setLabel("bottom", "Tick Index")

    img_combined = pg.ImageItem()
    img_combined.setLookupTable(FULL_LUT)
    p_combined.addItem(img_combined)
    ltp_line_combined = p_combined.plot([], [], pen=pg.mkPen(color=(0, 0, 255), width=2))

    comp_plots, comp_imgs, comp_ltps = [], [], []
    stat_plots, signal_plots = [], []
    min_lines, max_lines = [], []

    for i, (label, arr_key, side, code) in enumerate(COMPONENT_SPECS):
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

        # Min/max active LH/HL boundary lines (previously notebook-only).
        min_lines.append(p.plot([], [], pen=pg.mkPen(color=(128, 0, 128), width=4.0), name="Min LH/HL"))
        max_lines.append(p.plot([], [], pen=pg.mkPen(color=(0, 0, 0), width=4.0), name="Max LH/HL"))

        sp = win_stats.addPlot(row=row * 2, col=col, title=f"{label} Max/Mean/Min")
        sp.showGrid(x=True, y=True, alpha=0.15)
        sp.setLabel("left", "Value")
        sp.setLabel("bottom", "Tick Index")
        sp_max   = sp.plot([], [], pen=pg.mkPen(color=(0, 0, 255),  width=2), name="Max")
        sp_mean  = sp.plot([], [], pen=pg.mkPen(color=(0, 200, 0),   width=2), name="Mean")
        sp_min   = sp.plot([], [], pen=pg.mkPen(color=(255, 200, 0), width=2), name="Min")
        stat_plots.append((sp, sp_max, sp_mean, sp_min))

        sp_cnt = win_stats.addPlot(row=row * 2 + 1, col=col, title=f"{label} Cnt>0")
        sp_cnt.showGrid(x=True, y=True, alpha=0.15)
        sp_cnt.setLabel("left", "Count")
        sp_cnt.setLabel("bottom", "Tick Index")
        sp_cnt.setXLink(sp)
        cnt_line = sp_cnt.plot([], [], pen=pg.mkPen(color=(200, 0, 200), width=2), name="Cnt>0")
        signal_plots.append((sp_cnt, cnt_line))

    # Cumulative pos% differentials: buy-vs-sell bias, and HL-vs-LH skew per side.
    p_bias = win_pct.addPlot(row=0, col=0, title="(hl_buy+lh_buy) - (hl_sell+lh_sell) pos%")
    p_bias.showGrid(x=True, y=True, alpha=0.15)
    p_bias.setLabel("left", "pos% diff")
    p_bias.setLabel("bottom", "Tick Index")
    p_bias.addLine(y=0, pen=pg.mkPen(color=(150, 150, 150), width=1, style=QtCore.Qt.PenStyle.DashLine))
    bias_line = p_bias.plot([], [], pen=pg.mkPen(color=(0, 0, 255), width=2), name="buy - sell")

    p_skew = win_pct.addPlot(row=1, col=0, title="hl - lh pos% (per side)")
    p_skew.showGrid(x=True, y=True, alpha=0.15)
    p_skew.setLabel("left", "pos% diff")
    p_skew.setLabel("bottom", "Tick Index")
    p_skew.setXLink(p_bias)
    p_skew.addLine(y=0, pen=pg.mkPen(color=(150, 150, 150), width=1, style=QtCore.Qt.PenStyle.DashLine))
    skew_legend = p_skew.addLegend(offset=(10, 10))
    skew_buy_line  = p_skew.plot([], [], pen=pg.mkPen(color=(0, 150, 0), width=2), name="hl_buy - lh_buy")
    skew_sell_line = p_skew.plot([], [], pen=pg.mkPen(color=(150, 0, 0), width=2), name="hl_sell - lh_sell")

    # Spike markers on the combined ("main") graph: one colour per signal,
    # with a legend mapping colour -> signal.
    legend = p_combined.addLegend(offset=(10, 10))
    drop_scatters, max_scatters = [], []
    for (label, *_), color in zip(COMPONENT_SPECS, SPIKE_COLORS):
        sc = pg.ScatterPlotItem(
            size=12, symbol='o',
            brush=pg.mkBrush(*color),
            pen=pg.mkPen(color=(0, 0, 0, 150), width=0.5),
        )
        p_combined.addItem(sc)
        legend.addItem(sc, f"{label} drop (>{COUNT_DROP_THRESHOLD:g})")
        drop_scatters.append(sc)

        xc = pg.ScatterPlotItem(
            size=14, symbol='x',
            pen=pg.mkPen(color=color, width=2),
            brush=pg.mkBrush(*color),
        )
        p_combined.addItem(xc)
        legend.addItem(xc, f"{label} spike (|d1|>{SPIKE_THRESHOLD:g})")
        max_scatters.append(xc)

    master = comp_plots[0]
    for p in comp_plots[1:]:
        p.setXLink(master)
        p.setYLink(master)

    for w in (win_combined, win_components, win_stats, win_pct):
        w.show()

    signal_visible = [True]

    toggle_btn = QtWidgets.QPushButton("Hide Cnt")
    toggle_btn.setCheckable(True)

    def _toggle_signals(checked):
        visible = not checked
        signal_visible[0] = visible
        toggle_btn.setText("Show Cnt" if checked else "Hide Cnt")
        for sp_cnt, _ in signal_plots:
            sp_cnt.setVisible(visible)

    toggle_btn.toggled.connect(_toggle_signals)
    toggle_btn.setFixedWidth(120)
    toggle_btn.show()

    w = {
        "app": app,
        "windows": (win_combined, win_components, win_stats, win_pct),
        "p_combined": p_combined,
        "img_combined": img_combined,
        "ltp_line_combined": ltp_line_combined,
        "comp_plots": comp_plots,
        "comp_imgs": comp_imgs,
        "comp_ltps": comp_ltps,
        "stat_plots": stat_plots,
        "signal_plots": signal_plots,
        "min_lines": min_lines,
        "max_lines": max_lines,
        "drop_scatters": drop_scatters,
        "max_scatters": max_scatters,
        "signal_visible": signal_visible,
        "bias_line": bias_line,
        "skew_buy_line": skew_buy_line,
        "skew_sell_line": skew_sell_line,
        "skew_legend": skew_legend,
        # held so Qt doesn't garbage-collect the button and its connection
        "toggle_btn": toggle_btn,
    }

    if sync_ranges:
        _link_all_ranges(w)

    return w


def _link_all_ranges(w):
    """Pan/zoom one plot, move them all. Guarded against the feedback loop."""
    all_plots = [w["p_combined"]] + w["comp_plots"]
    guard = {"busy": False}

    def _sync(src_vb, ranges):
        if guard["busy"]:
            return
        guard["busy"] = True
        try:
            (x0, x1), (y0, y1) = ranges
            for p in all_plots:
                vb = p.getViewBox()
                if vb is src_vb:
                    continue
                vb.setXRange(x0, x1, padding=0)
                vb.setYRange(y0, y1, padding=0)
        finally:
            guard["busy"] = False

    for p in all_plots:
        p.getViewBox().sigRangeChanged.connect(_sync)
    w["_range_sync"] = _sync  # keep the callback alive


def render(instance, w):
    """Compute one frame from instance's current state and push it into the scene."""
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
    heatmap[~(valid_buy | valid_sell)] = 16

    min_ltp = float(instance.base_ltp + min_idx)
    max_ltp = float(instance.base_ltp + max_idx)
    rect = QRectF(0, min_ltp, rows_used, max(1.0, max_ltp - min_ltp + 1.0))

    w["img_combined"].setImage(heatmap, autoLevels=False)
    w["img_combined"].setRect(rect)

    ltp_vals  = instance.ltpdf[:rows_used, 0].astype(float)
    valid_ltp = np.isfinite(ltp_vals) & (ltp_vals != 0)
    x_ltp = np.arange(rows_used, dtype=float)[valid_ltp]
    y_ltp = ltp_vals[valid_ltp]
    w["ltp_line_combined"].setData(x_ltp, y_ltp)
    w["p_combined"].setYRange(min_ltp, max_ltp, padding=0)

    arrays = [hl_buy, lh_buy, hl_sell, lh_sell]
    codes  = [8, 4, 2, 1]
    row_x = np.arange(rows_used, dtype=float)

    for i, (arr, code) in enumerate(zip(arrays, codes)):
        w["comp_imgs"][i].setImage(_make_component_map(arr, code), autoLevels=False)
        w["comp_imgs"][i].setRect(rect)
        w["comp_ltps"][i].setData(x_ltp, y_ltp)

        arr_f = arr.astype(float)
        has_data = ~np.all(np.isnan(arr_f), axis=1)
        row_max  = np.full(arr_f.shape[0], np.nan)
        row_mean = np.full(arr_f.shape[0], np.nan)
        row_min  = np.full(arr_f.shape[0], np.nan)
        if has_data.any():
            row_max[has_data]  = np.nanmax(arr_f[has_data],  axis=1)
            row_mean[has_data] = np.nanmean(arr_f[has_data], axis=1)
            row_min[has_data]  = np.nanmin(arr_f[has_data],  axis=1)

        # Min/max active LH/HL boundary lines on the component plot.
        min_series, max_series = _active_bounds_series(arr_f, min_idx, instance.base_ltp)
        fin_min, fin_max = np.isfinite(min_series), np.isfinite(max_series)
        w["min_lines"][i].setData(row_x[fin_min], min_series[fin_min])
        w["max_lines"][i].setData(row_x[fin_max], max_series[fin_max])

        if has_data.any():
            xv = row_x[has_data]
            _, sp_max, sp_mean, sp_min = w["stat_plots"][i]
            sp_max.setData(xv,  row_max[has_data])
            sp_mean.setData(xv, row_mean[has_data])
            sp_min.setData(xv,  row_min[has_data])

            if w["signal_visible"][0]:
                _, cnt_line = w["signal_plots"][i]
                cnt_line.setData(xv, (arr_f[has_data] > 0).sum(axis=1))

        # Mark ticks where count>0 suddenly drops by more than the
        # threshold on the combined LTP graph, colour-coded per signal.
        cnt = (arr_f > 0).sum(axis=1).astype(float)
        d_cnt = _first_order_diff(cnt)
        drop_idx = np.nonzero((d_cnt < -COUNT_DROP_THRESHOLD) & valid_ltp)[0]
        w["drop_scatters"][i].setData(drop_idx.astype(float), ltp_vals[drop_idx])

        sig_x = _first_order_diff(row_max)
        spike_idx = np.nonzero((np.abs(sig_x) > SPIKE_THRESHOLD) & valid_ltp)[0]
        w["max_scatters"][i].setData(spike_idx.astype(float), ltp_vals[spike_idx])

    # Cumulative pos% differentials. arrays order is hl_buy, lh_buy, hl_sell, lh_sell.
    pct_hl_buy, pct_lh_buy, pct_hl_sell, pct_lh_sell = (
        _cumulative_pos_pct(a.astype(float)) for a in arrays
    )
    bias = (pct_hl_buy + pct_lh_buy) - (pct_hl_sell + pct_lh_sell)
    skew_buy  = pct_hl_buy  - pct_lh_buy
    skew_sell = pct_hl_sell - pct_lh_sell
    for key, series in (("bias_line", bias),
                        ("skew_buy_line", skew_buy),
                        ("skew_sell_line", skew_sell)):
        fin = np.isfinite(series)
        w[key].setData(row_x[fin], series[fin])


def delta_graph(instance):
    """
    Launch the live 4-window PyQtGraph visualization.
    Blocks on app.exec() — must be called from the main thread.
    """
    while instance.curr_time_idx == 0:
        time.sleep(0.05)

    w = build_windows()
    timer = pg.QtCore.QTimer()
    timer.timeout.connect(lambda: render(instance, w))
    timer.start(50)
    w["app"].exec()


def show_static(instance):
    """
    Render an already-parsed instance once and block on the Qt loop.
    Same scene as the live graph; no polling.
    """
    w = build_windows(sync_ranges=True)
    render(instance, w)
    w["app"].exec()
