import asyncio
import json
import threading
import websockets
from config import r, STOCKS, EXCHANGE

HOST = "ws://139.59.32.232:8765/ws"


async def _run_async_producer(stock: str):
    ws = await websockets.connect(HOST)
    async with ws:
        await ws.send(json.dumps({"stock": stock}))
        async for message in ws:
            if r.get('end') == 'true':
                break
            tick = json.loads(message)
            if 'last_price' not in tick:
                continue
            r.xadd(stock, tick, maxlen=10000)


def start_producer(stock: str) -> threading.Thread:
    def _run():
        asyncio.run(_run_async_producer(stock))

    t = threading.Thread(target=_run, name="WsProducer", daemon=True)
    t.start()
    return t


if __name__ == "__main__":
    stock = f"{EXCHANGE}:{STOCKS[0]}"
    start_producer(stock).join()
