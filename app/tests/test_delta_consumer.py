import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import threading
from unittest.mock import MagicMock, patch
from StockAnalyser import Delta_analysis

TICK = {
    'timestamp': '09:15:17', 'stonk': 'NSE:RELIANCE',
    'last_price': '710.0', 'last_traded_quantity': '5',
    'average_traded_price': '710.0', 'volume_traded': '100',
    'total_buy_quantity': '100', 'total_sell_quantity': '200',
    'open': '710.0', 'high': '710.0', 'low': '710.0', 'close': '709.0',
    'change': '0.1',
    'buy_price_1': '710.05', 'buy_qty_1': '100', 'buy_orders_1': '1',
    'sell_price_1': '711.0',  'sell_qty_1': '100', 'sell_orders_1': '1',
    'buy_price_2': '709.9',  'buy_qty_2': '50',  'buy_orders_2': '1',
    'sell_price_2': '711.5',  'sell_qty_2': '50',  'sell_orders_2': '1',
    'buy_price_3': '709.8',  'buy_qty_3': '30',  'buy_orders_3': '1',
    'sell_price_3': '712.0',  'sell_qty_3': '30',  'sell_orders_3': '1',
    'buy_price_4': '709.7',  'buy_qty_4': '20',  'buy_orders_4': '1',
    'sell_price_4': '712.5',  'sell_qty_4': '20',  'sell_orders_4': '1',
    'buy_price_5': '709.6',  'buy_qty_5': '10',  'buy_orders_5': '1',
    'sell_price_5': '713.0',  'sell_qty_5': '10',  'sell_orders_5': '1',
}


def test_consumer_calls_parse_for_each_claimed_message():
    mock_r = MagicMock()
    mock_r.xautoclaim.return_value = (None, [('id1', TICK), ('id2', TICK)], None)
    mock_r.xreadgroup.return_value = None
    calls = [0]

    def fake_get(key):
        calls[0] += 1
        return 'true' if calls[0] > 2 else 'false'

    mock_r.get.side_effect = fake_get
    instance = MagicMock(spec=Delta_analysis)

    with patch('delta_consumer.r', mock_r):
        from delta_consumer import start_consumer
        t = start_consumer('NSE:RELIANCE', instance)
        t.join(timeout=3)

    assert instance.parse.call_count >= 2


def test_start_consumer_returns_thread():
    mock_r = MagicMock()
    mock_r.xautoclaim.return_value = (None, [], None)
    mock_r.xreadgroup.return_value = None
    mock_r.get.return_value = 'true'
    instance = MagicMock(spec=Delta_analysis)

    with patch('delta_consumer.r', mock_r):
        from delta_consumer import start_consumer
        t = start_consumer('NSE:RELIANCE', instance)
        t.join(timeout=3)
        assert isinstance(t, threading.Thread)
