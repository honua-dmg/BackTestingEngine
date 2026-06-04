import threading
import logging
from config import r


def _consumer_loop(stock: str, instance) -> None:
    group = stock
    consumer = stock
    try:
        r.xgroup_create(name=stock, groupname=group, mkstream=True, id='0')
    except Exception:
        pass  # group already exists

    logging.info(f"[CONSUMER] starting for {stock}")
    while r.get('end') != 'true':
        _, claimed, _ = r.xautoclaim(stock, group, consumer,
                                     min_idle_time=0, start_id='0-0')
        for msg_id, tick in claimed:
            instance.parse(tick)
            r.xack(stock, group, msg_id)

        new = r.xreadgroup(groupname=group, consumername=consumer,
                           streams={stock: '>'}, block=10)
        if new:
            for _, messages in new:
                for msg_id, tick in messages:
                    try:
                        instance.parse(tick)
                        r.xack(stock, group, msg_id)
                    except Exception as e:
                        logging.error(f"[CONSUMER] tick error: {e}")

    logging.info(f"[CONSUMER] stopped for {stock}")


def start_consumer(stock: str, instance) -> threading.Thread:
    t = threading.Thread(target=_consumer_loop, args=(stock, instance),
                         name="DeltaConsumer", daemon=True)
    t.start()
    return t
