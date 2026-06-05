import asyncio
import json
import threading
import websockets
from config import r, STOCKS, EXCHANGE
import time
HOST = "ws://139.59.32.232:8765/ws"


async def _run_async_producer(stock: str):
    ws = await websockets.connect(HOST)
    async with ws:
        await ws.send(json.dumps({"stock": stock}))
        async for message in ws:
            if r.get('end') == 'true':
                break
            tick = json.loads(message)
            print((type(tick), tick.keys()))
            time.sleep(1)
            if 'data' not in tick.keys():
                continue
            #print(tick['data'])
            payload = {k: (json.dumps(v) if isinstance(v, (dict, list)) else v) for k, v in tick['data'].items()}
            r.xadd(stock, payload, maxlen=10000)
            print(f"[PRODUCER] received tick for {stock}: {payload.get('last_price')} at {payload.get('timestamp')}")


def start_producer(stock: str) -> threading.Thread:
    def _run():
        asyncio.run(_run_async_producer(stock))

    t = threading.Thread(target=_run, name="WsProducer", daemon=True)
    t.start()
    return t


if __name__ == "__main__":
    stock = f"{EXCHANGE}:{STOCKS[0]}"
    start_producer(stock).join()
