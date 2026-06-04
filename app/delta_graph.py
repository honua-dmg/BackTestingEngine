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
