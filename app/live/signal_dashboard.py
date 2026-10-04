"""
Live terminal dashboard: tails the mark-detector signal stream and prints
each event as it arrives. Read-only, no consumer group (single reader, no
ack/resume needed for a print-only view) -- simplest thing that works.
"""
import logging
import threading

from config import r, EXCHANGE, STOCKS


def _format_event(fields: dict) -> str:
    return (
        f"{fields.get('mark_type', '?').upper():5} {fields.get('triggering_signal', '?'):8} "
        f"tick={fields.get('tick')} ltp={fields.get('ltp')} | "
        f"hl_buy {fields.get('hl_buy_pos%')}/{fields.get('hl_buy_neg%')}  "
        f"lh_buy {fields.get('lh_buy_pos%')}/{fields.get('lh_buy_neg%')}  "
        f"hl_sell {fields.get('hl_sell_pos%')}/{fields.get('hl_sell_neg%')}  "
        f"lh_sell {fields.get('lh_sell_pos%')}/{fields.get('lh_sell_neg%')}"
    )


def _dashboard_loop(stock: str) -> None:
    stream = f"{stock}:signals"
    logging.info(f"[DASHBOARD] watching {stream}")
    last_id = "$"  # only new messages from now on
    while r.get('end') != 'true':
        resp = r.xread({stream: last_id}, block=2000, count=10)
        if not resp:
            continue
        for _, messages in resp:
            for msg_id, fields in messages:
                last_id = msg_id
                print(_format_event(fields))
    logging.info("[DASHBOARD] stopped")


def start_dashboard(stock: str) -> threading.Thread:
    t = threading.Thread(target=_dashboard_loop, args=(stock,),
                         name="SignalDashboard", daemon=True)
    t.start()
    return t


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    stock = f"{EXCHANGE}:{STOCKS[0]}"
    try:
        _dashboard_loop(stock)
    except KeyboardInterrupt:
        logging.info("[DASHBOARD] stopped")
