import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np

from export_mark_stats import cumulative_pos_neg_pct, compute_mark_stats


def test_cumulative_pos_neg_pct_basic():
    arr = np.array([
        [1.0, -1.0],   # row 0: 1 pos, 1 neg
        [2.0, np.nan],  # row 1: 1 pos, still 1 neg overall
        [-3.0, -4.0],  # row 2: 0 pos, 2 neg
    ])
    pct_pos, pct_neg = cumulative_pos_neg_pct(arr)
    assert np.allclose(pct_pos, [50.0, 66.66666667, 40.0])
    assert np.allclose(pct_neg, 100.0 - pct_pos)


def test_cumulative_pos_neg_pct_all_nan_row_no_warning():
    arr = np.full((2, 3), np.nan)
    with np.errstate(all='raise'):
        pct_pos, pct_neg = cumulative_pos_neg_pct(arr)
    assert np.all(np.isnan(pct_pos))
    assert np.all(np.isnan(pct_neg))


class _FakeInstance:
    """Minimal stand-in for Delta_analysis, just the attributes compute_mark_stats reads."""

    def __init__(self, lh_buy, hl_buy, lh_sell, hl_sell, ltp):
        self.curr_time_idx = lh_buy.shape[0]
        self.WIDTH = lh_buy.shape[1]
        self.lowHigh = {'buy': lh_buy, 'sell': lh_sell}
        self.highLow = {'buy': hl_buy, 'sell': hl_sell}
        self.ltpdf = np.column_stack([ltp, np.zeros_like(ltp), np.zeros_like(ltp), np.zeros_like(ltp)])
        self.aggdf_buy = (~np.isnan(lh_buy)).sum(axis=0)
        self.aggdf_sell = (~np.isnan(lh_sell)).sum(axis=0)


def test_compute_mark_stats_empty_instance_returns_empty_df():
    inst = _FakeInstance(
        lh_buy=np.full((0, 3), np.nan),
        hl_buy=np.full((0, 3), np.nan),
        lh_sell=np.full((0, 3), np.nan),
        hl_sell=np.full((0, 3), np.nan),
        ltp=np.zeros(0),
    )
    df = compute_mark_stats(inst)
    assert df.empty


def test_compute_mark_stats_detects_a_count_drop():
    rows, cols = 15, 20  # cols must exceed COUNT_DROP_THRESHOLD (10) for a drop to be detectable
    rng = np.random.default_rng(0)
    lh_buy = rng.uniform(-1, 1, size=(rows, cols))
    hl_buy = rng.uniform(-1, 1, size=(rows, cols))
    lh_sell = rng.uniform(-1, 1, size=(rows, cols))
    hl_sell = rng.uniform(-1, 1, size=(rows, cols))

    # Force a sharp count>0 drop in hl_buy at tick 10: all-positive before, all-negative after.
    hl_buy[:10, :] = 1.0
    hl_buy[10:, :] = -1.0

    ltp = np.full(rows, 100.0)

    inst = _FakeInstance(lh_buy, hl_buy, lh_sell, hl_sell, ltp)
    df = compute_mark_stats(inst)

    assert not df.empty
    drops = df[(df["mark_type"] == "drop") & (df["triggering_signal"] == "hl_buy")]
    assert (drops["tick"] == 10).any()
