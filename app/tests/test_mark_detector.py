import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np

from live.mark_detector import MarkDetector, COUNT_DROP_THRESHOLD, SPIKE_THRESHOLD


class _FakeInstance:
    """Minimal stand-in for Delta_analysis: only what MarkDetector.update() reads.
    push_row() appends one row to all four signal arrays and bumps curr_time_idx,
    the same shape as what Delta_analysis.parse()/update_signals() produces."""

    def __init__(self, width):
        self.width = width
        self.curr_time_idx = 0
        self.highLow = {'buy': np.empty((0, width)), 'sell': np.empty((0, width))}
        self.lowHigh = {'buy': np.empty((0, width)), 'sell': np.empty((0, width))}
        self.ltpdf = np.empty((0, 4))

    def push_row(self, ltp, hl_buy=None, lh_buy=None, hl_sell=None, lh_sell=None):
        def row(v):
            return np.full((1, self.width), np.nan) if v is None else np.asarray(v).reshape(1, -1)

        self.highLow['buy'] = np.vstack([self.highLow['buy'], row(hl_buy)])
        self.highLow['sell'] = np.vstack([self.highLow['sell'], row(hl_sell)])
        self.lowHigh['buy'] = np.vstack([self.lowHigh['buy'], row(lh_buy)])
        self.lowHigh['sell'] = np.vstack([self.lowHigh['sell'], row(lh_sell)])
        self.ltpdf = np.vstack([self.ltpdf, [[ltp, 0, 0, 0]]])
        self.curr_time_idx += 1


def test_no_marks_before_any_data():
    inst = _FakeInstance(width=5)
    assert MarkDetector().update(inst) == []


def test_first_tick_never_triggers():
    """No previous count/max to diff against yet."""
    inst = _FakeInstance(width=20)
    inst.push_row(ltp=100.0, hl_buy=[1.0] * 20)
    assert MarkDetector().update(inst) == []


def test_invalid_ltp_produces_no_marks():
    inst = _FakeInstance(width=20)
    inst.push_row(ltp=0.0, hl_buy=[1.0] * 20)
    assert MarkDetector().update(inst) == []


def test_detects_count_drop():
    width = 20  # must exceed COUNT_DROP_THRESHOLD (10) for a drop to be detectable
    inst = _FakeInstance(width=width)
    detector = MarkDetector()

    inst.push_row(ltp=100.0, hl_buy=[1.0] * width)
    assert detector.update(inst) == []  # priming tick

    inst.push_row(ltp=100.0, hl_buy=[-1.0] * width)  # count>0 drops from 20 to 0
    events = detector.update(inst)

    drops = [e for e in events if e["mark_type"] == "drop" and e["triggering_signal"] == "hl_buy"]
    assert len(drops) == 1
    assert drops[0]["tick"] == 1
    assert drops[0]["ltp"] == 100.0


def test_detects_spike():
    width = 3
    inst = _FakeInstance(width=width)
    detector = MarkDetector()

    inst.push_row(ltp=100.0, lh_sell=[0.0] * width)
    assert detector.update(inst) == []  # priming tick

    inst.push_row(ltp=100.0, lh_sell=[SPIKE_THRESHOLD + 1] * width)  # max jumps well past threshold
    events = detector.update(inst)

    spikes = [e for e in events if e["mark_type"] == "spike" and e["triggering_signal"] == "lh_sell"]
    assert len(spikes) == 1


def test_no_trigger_below_thresholds():
    width = 20
    inst = _FakeInstance(width=width)
    detector = MarkDetector()

    inst.push_row(ltp=100.0, hl_buy=[1.0] * width)
    detector.update(inst)

    # Drop by fewer cells than COUNT_DROP_THRESHOLD, max barely moves.
    small_drop = [1.0] * (width - int(COUNT_DROP_THRESHOLD) + 1) + [-1.0] * (int(COUNT_DROP_THRESHOLD) - 1)
    inst.push_row(ltp=100.0, hl_buy=small_drop)
    assert detector.update(inst) == []


def test_event_stats_bundle_has_all_signals():
    width = 20
    inst = _FakeInstance(width=width)
    detector = MarkDetector()

    inst.push_row(ltp=100.0, hl_buy=[1.0] * width, lh_buy=[1.0] * width,
                  hl_sell=[1.0] * width, lh_sell=[1.0] * width)
    detector.update(inst)

    inst.push_row(ltp=100.0, hl_buy=[-1.0] * width, lh_buy=[1.0] * width,
                  hl_sell=[1.0] * width, lh_sell=[1.0] * width)
    events = detector.update(inst)

    assert events, "expected the hl_buy count drop to trigger an event"
    evt = events[0]
    for name in ("hl_buy", "lh_buy", "hl_sell", "lh_sell"):
        assert f"{name}_pos%" in evt
        assert f"{name}_neg%" in evt
        assert abs(evt[f"{name}_pos%"] + evt[f"{name}_neg%"] - 100.0) < 1e-9
