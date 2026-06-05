import asyncio
import json
import logging
import threading
import websockets
from config import r, STOCKS, EXCHANGE

HOST = "ws://139.59.32.232:8765/ws"


async def _run_async_producer(stock: str):
    backoff = 1
    while r.get('end') != 'true':
        try:
            async with websockets.connect(
                HOST,
                ping_interval=20,
                ping_timeout=60,
                close_timeout=10,
            ) as ws:
                await ws.send(json.dumps({"stock": stock}))
                backoff = 1
                async for message in ws:
                    if r.get('end') == 'true':
                        return
                    tick = json.loads(message)
                    if 'data' not in tick:
                        continue
                    payload = {k: (json.dumps(v) if isinstance(v, (dict, list)) else v)
                               for k, v in tick['data'].items()}
                    r.xadd(stock, payload, maxlen=10000)
                    print(f"[PRODUCER] {stock}: {payload.get('last_price')} @ {payload.get('timestamp')}")
        except websockets.exceptions.ConnectionClosedError as e:
            logging.warning(f"[PRODUCER] connection closed ({e}), reconnecting in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
        except Exception as e:
            logging.error(f"[PRODUCER] unexpected error: {e}, reconnecting in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


def start_producer(stock: str) -> threading.Thread:
    def _run():
        asyncio.run(_run_async_producer(stock))

    t = threading.Thread(target=_run, name="WsProducer", daemon=True)
    t.start()
    return t


if __name__ == "__main__":
    stock = f"{EXCHANGE}:{STOCKS[0]}"
    start_producer(stock).join()
