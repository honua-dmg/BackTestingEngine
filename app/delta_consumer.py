import json
import threading
import logging
from config import r
from StockAnalyser import Delta_analysis
from mark_detector import MarkDetector

SIGNAL_STREAM_MAXLEN = 10000


def _publish_marks(stock: str, detector: MarkDetector, instance: Delta_analysis) -> None:
    # Guards both stages separately: a detection or publish failure must not be
    # reported as a tick error, and must never interrupt tick processing.
    try:
        events = detector.update(instance)
    except Exception as e:
        logging.error(f"[CONSUMER] mark detection error: {e}")
        return

    for evt in events:
        try:
            r.xadd(f"{stock}:signals", evt, maxlen=SIGNAL_STREAM_MAXLEN, approximate=True)
        except Exception as e:
            logging.error(f"[CONSUMER] signal publish error: {e}")


def _deserialize(tick: dict) -> dict:
    result = {}
    for k, v in tick.items():
        key = k.decode() if isinstance(k, bytes) else k
        val = v.decode() if isinstance(v, bytes) else v
        try:
            result[key] = json.loads(val)
        except (json.JSONDecodeError, TypeError):
            result[key] = val
    return result


def _consumer_loop(stock: str, instance:Delta_analysis) -> None:
    group = stock
    consumer = stock
    detector = MarkDetector()
    try:
        r.xgroup_create(name=stock, groupname=group, mkstream=True, id='0')
    except Exception:
        pass  # group already exists

    logging.info(f"[CONSUMER] starting for {stock}")
    while r.get('end') != 'true':
        _, claimed, _ = r.xautoclaim(stock, group, consumer,
                                     min_idle_time=0, start_id='0-0')
        for msg_id, tick in claimed:
            try:
                instance.parse(_deserialize(tick))
                r.xack(stock, group, msg_id)
                _publish_marks(stock, detector, instance)
            except Exception as e:
                logging.error(f"[CONSUMER] tick error: {e}")

        new = r.xreadgroup(groupname=group, consumername=consumer,
                           streams={stock: '>'}, block=10)
        if new:
            for _, messages in new:
                for msg_id, tick in messages:
                    try:
                        instance.parse(_deserialize(tick))
                        r.xack(stock, group, msg_id)
                        _publish_marks(stock, detector, instance)
                    except Exception as e:
                        logging.error(f"[CONSUMER] tick error: {e}")

    logging.info(f"[CONSUMER] stopped for {stock}")


def start_consumer(stock: str, instance) -> threading.Thread:
    t = threading.Thread(target=_consumer_loop, args=(stock, instance),
                         name="DeltaConsumer", daemon=True)
    t.start()
    return t
