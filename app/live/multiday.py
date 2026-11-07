"""
Multi-day Delta_analysis instance for the live pipeline.

A second instance, seeded with the last few trading days from S3, that keeps
accumulating today's live ticks on top. It sits next to the daily instance and
gets its own combined graph, so today's flow can be read against the week's.

Seeding takes a while (download + parse every past tick), so it runs in a
background thread. Live ticks that arrive in the meantime are buffered and
replayed once the history is in, so the daily graph starts immediately and the
multi-day instance still sees every tick in order.
"""
import datetime as dt
import logging
import threading

from StockAnalyser import Delta_analysis

# How far back the multi-day instance reaches:
#   "tuesday" -> from the most recent Tuesday before today (weekly expiry)
#   an int N  -> the last N trading days before today
LOOKBACK = "tuesday"

# Give up looking for trading days further back than this many calendar days.
MAX_CALENDAR_LOOKBACK = 30

TUESDAY = 1  # date.weekday()


def lookback_dates(today, lookback, has_data):
    """
    Past trading dates to seed from, oldest first, today excluded (today comes
    from the live feed). A trading day is a weekday has_data(date_str) accepts,
    so exchange holidays drop out without a calendar.
    """
    if lookback == "tuesday":
        back = (today.weekday() - TUESDAY) % 7 or 7
        candidates = [today - dt.timedelta(days=i) for i in range(back, 0, -1)]
        return [d.isoformat() for d in candidates
                if d.weekday() < 5 and has_data(d.isoformat())]

    n = int(lookback)
    picked = []
    for i in range(1, MAX_CALENDAR_LOOKBACK + 1):
        if len(picked) >= n:
            break
        d = today - dt.timedelta(days=i)
        if d.weekday() < 5 and has_data(d.isoformat()):
            picked.append(d.isoformat())
    return picked[::-1]


def _start_new_day(instance, day_starts, date_str):
    # volume_traded is cumulative per day, so the cleaner's baseline resets at
    # every day boundary (same as the notebook's multi-day loop).
    instance.cleaner.last_traded_vol = 0
    instance.cleaner.last_ltp = None
    day_starts[date_str] = instance.curr_time_idx


class MultiDayFeed:
    """
    Wraps the multi-day instance for the consumer. parse() buffers until
    attach() hands over the seeded instance, then forwards straight through.
    """

    def __init__(self):
        self.instance = None
        self.day_starts = {}
        self.failed = False
        self._pending = []
        self._lock = threading.Lock()

    def parse(self, tick):
        if self.instance is not None:
            self.instance.parse(tick)
            return
        with self._lock:
            if self.instance is not None:
                self.instance.parse(tick)
            elif not self.failed:
                self._pending.append(tick)

    def attach(self, instance, day_starts, today_str):
        """Mark today's start, replay the buffered live ticks, go live."""
        with self._lock:
            _start_new_day(instance, day_starts, today_str)
            for tick in self._pending:
                instance.parse(tick)
            self._pending.clear()
            self.day_starts = day_starts
            self.instance = instance

    def fail(self):
        with self._lock:
            self.failed = True
            self._pending.clear()


def seed_instance(dates):
    """Fresh instance with every date's ticks parsed in, plus where each day starts."""
    # Imported here: these pull in kiteconnect/S3 and call logging.basicConfig
    # at import, which would otherwise override delta_main's logging setup.
    from backtest.export_mark_stats import download_file
    from backtest.simulator import get_all_tick_data

    instance = Delta_analysis()
    day_starts = {}
    for date_str in dates:
        download_file(date_str)
        df = get_all_tick_data(date_str)
        if df is None or df.empty:
            continue
        _start_new_day(instance, day_starts, date_str)
        for _, row in df.iterrows():
            instance.parse(row.to_dict())
        logging.info(f"[MULTIDAY] seeded {date_str}: {instance.curr_time_idx} ticks total")
    return instance, day_starts


def _seed_worker(feed, lookback, today):
    try:
        from backtest.export_mark_stats import s3_key_exists
        dates = lookback_dates(today, lookback, s3_key_exists)
        logging.info(f"[MULTIDAY] seeding from {dates}")
        instance, day_starts = seed_instance(dates)
        feed.attach(instance, day_starts, today.isoformat())
        logging.info(f"[MULTIDAY] live with {len(day_starts)} days, {instance.curr_time_idx} ticks")
    except Exception:
        logging.exception("[MULTIDAY] seeding failed; multi-day graph disabled")
        feed.fail()


def start_multiday(lookback=LOOKBACK, today=None):
    """Return a MultiDayFeed and the thread seeding it."""
    feed = MultiDayFeed()
    today = today or dt.date.today()
    t = threading.Thread(target=_seed_worker, args=(feed, lookback, today),
                         name="MultiDaySeed", daemon=True)
    t.start()
    return feed, t
