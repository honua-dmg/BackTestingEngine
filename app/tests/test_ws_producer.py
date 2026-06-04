import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import asyncio
import json
import threading
from unittest.mock import MagicMock, patch

TICK = json.dumps({
    'timestamp': '09:15:17', 'stonk': 'NSE:RELIANCE',
    'last_price': '2345.0', 'volume_traded': '100',
})


def _fake_ws_connect(messages):
    async def fake_connect(url):
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
    mock_r.get.return_value = 'false'

    with patch('ws_producer.websockets.connect', new=_fake_ws_connect([TICK, TICK])), \
         patch('ws_producer.r', mock_r):
        from ws_producer import _run_async_producer
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

    with patch('ws_producer.websockets.connect', new=_fake_ws_connect([TICK, TICK, TICK])), \
         patch('ws_producer.r', mock_r):
        from ws_producer import _run_async_producer
        asyncio.run(_run_async_producer('NSE:RELIANCE'))

    assert mock_r.xadd.call_count <= 1


def test_start_producer_returns_thread():
    mock_r = MagicMock()
    mock_r.get.return_value = 'false'

    with patch('ws_producer.websockets.connect', new=_fake_ws_connect([])), \
         patch('ws_producer.r', mock_r):
        from ws_producer import start_producer
        t = start_producer('NSE:RELIANCE')
        t.join(timeout=3)
        assert isinstance(t, threading.Thread)
