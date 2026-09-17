"""
Incremental (O(1) per tick) drop/spike mark detection for a live Delta_analysis
instance. Mirrors the batch logic in export_mark_stats.py / the notebook's
mark-stats cell, but keeps running state instead of recomputing over full
history, so it's cheap to call once per tick from delta_consumer.py.
"""
import numpy as np

# Same thresholds as export_mark_stats.py / delta_graph.py / the notebook.
COUNT_DROP_THRESHOLD = 10.0
SPIKE_THRESHOLD = 5.0

COMPONENT_NAMES = ["hl_buy", "lh_buy", "hl_sell", "lh_sell"]


class MarkDetector:
    """One instance per stock. Call update(instance) after each
    Delta_analysis.parse(); returns any marks triggered by that tick."""

    def __init__(self):
        self._prev_count = {name: np.nan for name in COMPONENT_NAMES}
        self._prev_max = {name: np.nan for name in COMPONENT_NAMES}
        self._cum_valid = {name: 0.0 for name in COMPONENT_NAMES}
        self._cum_pos = {name: 0.0 for name in COMPONENT_NAMES}

    def update(self, instance):
        row = instance.curr_time_idx - 1
        if row < 0:
            return []

        ltp = float(instance.ltpdf[row, 0])
        if not (np.isfinite(ltp) and ltp != 0):
            return []

        rows = {
            "hl_buy": instance.highLow['buy'][row],
            "lh_buy": instance.lowHigh['buy'][row],
            "hl_sell": instance.highLow['sell'][row],
            "lh_sell": instance.lowHigh['sell'][row],
        }

        triggers = []  # (name, mark_type)
        for name, arr_row in rows.items():
            valid_mask = ~np.isnan(arr_row)
            pos_mask = valid_mask & (arr_row >= 0)
            cnt = int((arr_row > 0).sum())
            max_val = float(np.nanmax(arr_row)) if valid_mask.any() else np.nan

            self._cum_valid[name] += float(valid_mask.sum())
            self._cum_pos[name] += float(pos_mask.sum())

            d_cnt = cnt - self._prev_count[name]
            d_max = max_val - self._prev_max[name]

            if d_cnt < -COUNT_DROP_THRESHOLD:
                triggers.append((name, "drop"))
            if abs(d_max) > SPIKE_THRESHOLD:
                triggers.append((name, "spike"))

            self._prev_count[name] = cnt
            self._prev_max[name] = max_val

        if not triggers:
            return []

        stats = {}
        for name in COMPONENT_NAMES:
            cv, cp = self._cum_valid[name], self._cum_pos[name]
            if cv > 0:
                pct_pos = 100.0 * cp / cv
                stats[f"{name}_pos%"] = round(pct_pos, 2)
                stats[f"{name}_neg%"] = round(100.0 - pct_pos, 2)
            else:
                stats[f"{name}_pos%"] = float('nan')
                stats[f"{name}_neg%"] = float('nan')

        events = []
        for name, mark_type in triggers:
            events.append({
                "tick": row,
                "ltp": ltp,
                "mark_type": mark_type,
                "triggering_signal": name,
                **stats,
            })
        return events
