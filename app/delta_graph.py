import time
import numpy as np
import pyqtgraph as pg
from pyqtgraph.Qt import QtCore, QtWidgets
QRectF = QtCore.QRectF

# A "drop" is a tick-to-tick fall in the count>0 of more than this many cells.
COUNT_DROP_THRESHOLD = 10
# One colour per signal: buys green shades, sells red shades.
SPIKE_COLORS = [
    (  0, 150,   0),   # hl_buy  - dark green
    (100, 220, 100),   # lh_buy  - light green
    (150,   0,   0),   # hl_sell - dark red
    (240, 100, 100),   # lh_sell - light red
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

    # Spike markers on the combined ("main") graph: one colour per signal,
    # with a legend mapping colour -> signal.
    legend = p_combined.addLegend(offset=(10, 10))
    spike_scatters = []
    for (label, *_), color in zip(component_specs, SPIKE_COLORS):
        sc = pg.ScatterPlotItem(
            size=12, symbol='o',
            brush=pg.mkBrush(*color),
            pen=pg.mkPen(color=(0, 0, 0, 150), width=0.5),
        )
        p_combined.addItem(sc)
        legend.addItem(sc, f"{label} drop (>{COUNT_DROP_THRESHOLD:g})")
        spike_scatters.append(sc)

    master = comp_plots[0]
    for p in comp_plots[1:]:
        p.setXLink(master)
        p.setYLink(master)

    for w in (win_combined, win_components, win_stats):
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
        heatmap[~(valid_buy | valid_sell)] = 16

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
            has_data = ~np.all(np.isnan(arr_f), axis=1)
            row_max  = np.full(arr_f.shape[0], np.nan)
            row_mean = np.full(arr_f.shape[0], np.nan)
            row_min  = np.full(arr_f.shape[0], np.nan)
            if has_data.any():
                row_max[has_data]  = np.nanmax(arr_f[has_data],  axis=1)
                row_mean[has_data] = np.nanmean(arr_f[has_data], axis=1)
                row_min[has_data]  = np.nanmin(arr_f[has_data],  axis=1)

            valid = has_data
            if np.any(valid):
                xv = np.arange(rows_used, dtype=float)[valid]
                _, sp_max, sp_mean, sp_min = stat_plots[i]
                sp_max.setData(xv,  row_max[valid])
                sp_mean.setData(xv, row_mean[valid])
                sp_min.setData(xv,  row_min[valid])

                if signal_visible[0]:
                    _, cnt_line = signal_plots[i]
                    cnt_line.setData(xv, (arr_f[valid] > 0).sum(axis=1))

            # Mark ticks where count>0 suddenly drops by more than the
            # threshold on the combined LTP graph, colour-coded per signal.
            cnt = (arr_f > 0).sum(axis=1).astype(float)
            d_cnt = _first_order_diff(cnt)
            drop = (d_cnt < -COUNT_DROP_THRESHOLD) & valid_ltp
            drop_idx = np.nonzero(drop)[0]
            spike_scatters[i].setData(drop_idx.astype(float), ltp_vals[drop_idx])

    timer = pg.QtCore.QTimer()
    timer.timeout.connect(update)
    timer.start(50)
    app.exec()
