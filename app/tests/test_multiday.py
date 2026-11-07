import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import datetime as dt
from unittest.mock import MagicMock

from live.multiday import lookback_dates, MultiDayFeed

# 2026-11-05 is a Thursday.
THU = dt.date(2026, 11, 5)


def _all_days(_):
    return True


def test_tuesday_lookback_runs_from_last_tuesday_to_yesterday():
    assert lookback_dates(THU, "tuesday", _all_days) == ["2026-11-03", "2026-11-04"]


def test_tuesday_lookback_on_a_tuesday_reaches_the_previous_week():
    tue = dt.date(2026, 11, 10)
    assert lookback_dates(tue, "tuesday", _all_days) == [
        "2026-11-03", "2026-11-04", "2026-11-05", "2026-11-06", "2026-11-09",
    ]


def test_tuesday_lookback_skips_days_without_data():
    holiday = {"2026-11-03"}
    assert lookback_dates(THU, "tuesday", lambda d: d not in holiday) == ["2026-11-04"]


def test_trading_day_lookback_skips_weekends_and_holidays():
    holiday = {"2026-11-03"}
    assert lookback_dates(THU, 5, lambda d: d not in holiday) == [
        "2026-10-28", "2026-10-29", "2026-10-30", "2026-11-02", "2026-11-04",
    ]


def test_feed_buffers_until_attached_then_replays_in_order():
    feed = MultiDayFeed()
    feed.parse({"n": 1})
    feed.parse({"n": 2})

    inst = MagicMock()
    inst.curr_time_idx = 7
    feed.attach(inst, {"2026-11-04": 0}, "2026-11-05")

    # today's start recorded and the cleaner reset before the replay
    assert feed.day_starts == {"2026-11-04": 0, "2026-11-05": 7}
    assert inst.cleaner.last_traded_vol == 0
    assert inst.cleaner.last_ltp is None

    feed.parse({"n": 3})
    assert [c.args[0]["n"] for c in inst.parse.call_args_list] == [1, 2, 3]


def test_failed_feed_drops_ticks():
    feed = MultiDayFeed()
    feed.parse({"n": 1})
    feed.fail()
    feed.parse({"n": 2})
    assert feed._pending == []
