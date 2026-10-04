"""
Batch export of mark stats (drop/spike cumulative %pos/%neg) across every
date we have tick data for in cloud storage, from START_DATE to today.
Appends to one CSV, incrementally, skipping dates with no data in S3.

Run: python export_mark_stats.py
"""
import datetime as dt
import logging
import os

import numpy as np
import pandas as pd
from botocore.exceptions import ClientError

from config import EXCHANGE, FILEPATH, STOCKS, s3
from backtest.simulator import get_all_tick_data
from StockAnalyser import Delta_analysis

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

START_DATE = dt.date(2026, 3, 3)
BUCKET = "cashcow"
OUTPUT_CSV = f"{FILEPATH}/delta_components/{EXCHANGE}/{STOCKS[0]}/mark_stats_all.csv"

# A "drop" is a tick-to-tick fall in the count>0 of more than this many cells.
COUNT_DROP_THRESHOLD = 10.0
# A "spike" is |d1 of max| exceeding this many points (cross markers).
SPIKE_THRESHOLD = 5.0

COMPONENT_NAMES = ["hl_buy", "lh_buy", "hl_sell", "lh_sell"]


def get_active_price_bounds_local(inst):
    active = np.where((inst.aggdf_buy > 0) & (inst.aggdf_sell > 0))[0]
    if active.size == 0:
        return 0, max(0, inst.WIDTH - 1)
    return int(active[0]), int(active[-1])


def first_order_diff(series):
    """Difference between the current and immediate previous value."""
    series = np.asarray(series, dtype=float)
    return np.diff(series, prepend=np.nan)


def cumulative_pos_neg_pct(arr):
    """Per-row cumulative (tick 0..row) % of valid cells that are >=0 vs <0."""
    valid = ~np.isnan(arr)
    pos = valid & (arr >= 0)
    cum_valid = np.cumsum(valid.sum(axis=1).astype(float))
    cum_pos = np.cumsum(pos.sum(axis=1).astype(float))
    has_data = cum_valid > 0
    pct_pos = np.full(cum_valid.shape, np.nan)
    pct_pos[has_data] = 100.0 * cum_pos[has_data] / cum_valid[has_data]
    pct_neg = np.where(has_data, 100.0 - pct_pos, np.nan)
    return pct_pos, pct_neg


def s3_key_exists(date_str):
    key = f"{EXCHANGE}/{STOCKS[0]}/{date_str}.csv"
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code', '')
        if code in ('404', 'NoSuchKey', 'NotFound'):
            return False
        raise


def download_file(date_str):
    local_path = f"{FILEPATH}/{EXCHANGE}/{STOCKS[0]}/{date_str}.csv"
    if not os.path.exists(local_path):
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3.download_file(
            Bucket=BUCKET,
            Key=f"{EXCHANGE}/{STOCKS[0]}/{date_str}.csv",
            Filename=local_path,
        )
    return local_path


def compute_mark_stats(instance):
    """Pure logic, mirrors the notebook's mark-stats cell (no I/O)."""
    rows_used = int(instance.curr_time_idx)
    if rows_used <= 0:
        return pd.DataFrame()

    min_idx, max_idx = get_active_price_bounds_local(instance)
    col_slice = slice(min_idx, max_idx + 1)

    component_arrays = {
        "hl_buy": instance.highLow['buy'][:rows_used, col_slice],
        "lh_buy": instance.lowHigh['buy'][:rows_used, col_slice],
        "hl_sell": instance.highLow['sell'][:rows_used, col_slice],
        "lh_sell": instance.lowHigh['sell'][:rows_used, col_slice],
    }

    ltp_values = instance.ltpdf[:rows_used, 0].astype(float)
    valid_ltp = np.isfinite(ltp_values) & (ltp_values != 0)

    signal_cum_pct = {name: cumulative_pos_neg_pct(arr) for name, arr in component_arrays.items()}

    mark_stats = []

    def record_marks(tick_idx, mark_type, triggering_signal):
        for t in tick_idx:
            row = {"tick": int(t), "mark_type": mark_type, "triggering_signal": triggering_signal}
            for name in COMPONENT_NAMES:
                pct_pos, pct_neg = signal_cum_pct[name]
                row[f"{name}_pos%"] = round(float(pct_pos[t]), 2)
                row[f"{name}_neg%"] = round(float(pct_neg[t]), 2)
            mark_stats.append(row)

    for name in COMPONENT_NAMES:
        arr_slice = component_arrays[name][:rows_used, :].astype(float)
        has_data = ~np.all(np.isnan(arr_slice), axis=1)
        row_max = np.full(arr_slice.shape[0], np.nan)
        row_max[has_data] = np.nanmax(arr_slice[has_data], axis=1)

        cnt = (arr_slice > 0).sum(axis=1).astype(float)
        d_cnt = first_order_diff(cnt)
        drop_idx = np.nonzero((d_cnt < -COUNT_DROP_THRESHOLD) & valid_ltp)[0]

        sig_max = first_order_diff(row_max)
        spike_idx = np.nonzero((np.abs(sig_max) > SPIKE_THRESHOLD) & valid_ltp)[0]

        record_marks(drop_idx, "drop", name)
        record_marks(spike_idx, "spike", name)

    mark_stats_df = pd.DataFrame(mark_stats)
    if not mark_stats_df.empty:
        mark_stats_df = mark_stats_df.sort_values("tick").reset_index(drop=True)
    return mark_stats_df


def build_mark_stats_for_date(date_str):
    df = get_all_tick_data(date_str)
    instance = Delta_analysis()
    for _, row in df.iterrows():
        instance.parse(row.to_dict())
    return compute_mark_stats(instance)


def main():
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    wrote_header = os.path.exists(OUTPUT_CSV)

    current = START_DATE
    end_date = dt.date.today()
    while current <= end_date:
        date_str = current.isoformat()

        if not s3_key_exists(date_str):
            logging.info(f"[SKIP] {date_str}: no data in S3")
            current += dt.timedelta(days=1)
            continue

        try:
            download_file(date_str)
            day_df = build_mark_stats_for_date(date_str)
        except Exception as e:
            logging.error(f"[ERROR] {date_str}: {e}")
            current += dt.timedelta(days=1)
            continue

        if day_df.empty:
            logging.info(f"[SKIP] {date_str}: no marks")
        else:
            day_df.insert(0, "date", date_str)
            day_df.to_csv(OUTPUT_CSV, mode="a", header=not wrote_header, index=False)
            wrote_header = True
            logging.info(f"[OK] {date_str}: {len(day_df)} marks")

        current += dt.timedelta(days=1)

    logging.info(f"[DONE] wrote {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
