import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import asyncio
import json
import threading
from unittest.mock import MagicMock, patch

# The ws server wraps every tick as {"source": ..., "data": {...}}; ws_producer
# skips anything without a "data" key, so the envelope is required here.
TICK = json.dumps({
    'source': 'live',
    'data': {
        'timestamp': '09:15:17', 'stonk': 'NSE:RELIANCE',
        'last_price': '2345.0', 'volume_traded': '100',
    },
})


def _fake_ws_connect(messages):
    # Plain def, not async: production does `async with websockets.connect(...)`,
    # so this must return the async context manager itself, not a coroutine.
    # **kwargs absorbs the keepalive args (ping_interval/ping_timeout/close_timeout).
    def fake_connect(url, **kwargs):
        class FakeWS:
            def __aiter__(self): return self
            _idx = 0
            async def __anext__(self):
                if self._idx >= len(messages):
                    raise StopAsyncIteration
                msg = messages[self._idx]
                self._idx += 1
                return msg
            async def send(self, data): pass
            async def __aenter__(self): return self
            async def __aexit__(self, *a): pass
        return FakeWS()
    return fake_connect


def test_producer_pushes_each_message_to_redis():
    mock_r = MagicMock()
    # 'end' is read once by the outer loop, then once per message; it must stay
    # 'false' for those 3 reads and flip after, or the producer reconnects forever.
    reads = [0]

    def fake_get(key):
        reads[0] += 1
        return 'true' if reads[0] > 3 else 'false'

    mock_r.get.side_effect = fake_get

    with patch('live.ws_producer.websockets.connect', new=_fake_ws_connect([TICK, TICK])), \
         patch('live.ws_producer.r', mock_r):
        from live.ws_producer import _run_async_producer
        asyncio.run(_run_async_producer('NSE:RELIANCE'))

    assert mock_r.xadd.call_count == 2
    stream_key = mock_r.xadd.call_args_list[0][0][0]
    assert stream_key == 'NSE:RELIANCE'


def test_producer_stops_on_end_flag():
    mock_r = MagicMock()
    call_count = [0]

    def fake_get(key):
        call_count[0] += 1
        return 'true' if call_count[0] > 1 else 'false'

    mock_r.get.side_effect = fake_get

    with patch('live.ws_producer.websockets.connect', new=_fake_ws_connect([TICK, TICK, TICK])), \
         patch('live.ws_producer.r', mock_r):
        from live.ws_producer import _run_async_producer
        asyncio.run(_run_async_producer('NSE:RELIANCE'))

    assert mock_r.xadd.call_count <= 1


def test_start_producer_returns_thread():
    mock_r = MagicMock()
    # Flip 'end' immediately so the producer thread exits instead of busy-looping
    # reconnects for the rest of the test process.
    reads = [0]

    def fake_get(key):
        reads[0] += 1
        return 'true' if reads[0] > 1 else 'false'

    mock_r.get.side_effect = fake_get

    with patch('live.ws_producer.websockets.connect', new=_fake_ws_connect([])), \
         patch('live.ws_producer.r', mock_r):
        from live.ws_producer import start_producer
        t = start_producer('NSE:RELIANCE')
        t.join(timeout=3)
        assert isinstance(t, threading.Thread)
