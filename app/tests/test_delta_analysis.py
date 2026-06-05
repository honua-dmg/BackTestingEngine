import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np
from StockAnalyser import Delta_analysis

TICK = {
    'timestamp': '09:15:17', 'stonk': 'NSE:TATAMOTORS',
    'last_price': '710.0', 'last_traded_quantity': '5',
    'average_traded_price': '710.0', 'volume_traded': '100',
    'total_buy_quantity': '283158', 'total_sell_quantity': '542056',
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

TICK2 = {**TICK, 'volume_traded': '200'}


def test_parse_increments_curr_time_idx():
    inst = Delta_analysis()
    inst.parse(TICK)
    assert inst.curr_time_idx == 1


def test_parse_sets_base_ltp_on_first_tick():
    inst = Delta_analysis()
    inst.parse(TICK)
    assert inst.base_ltp is not None


def test_parse_zero_delta_is_noop():
    """Same volume_traded twice → delta_vol == 0 → parse returns early."""
    inst = Delta_analysis()
    inst.parse(TICK)
    inst.parse(TICK)
    assert inst.curr_time_idx == 1


def test_parse_accumulates_aggdf():
    inst = Delta_analysis()
    inst.parse(TICK)
    ltp_idx = int(float(TICK['last_price'])) - inst.base_ltp
    assert inst.aggdf_buy[ltp_idx] + inst.aggdf_sell[ltp_idx] > 0


def test_parse_populates_lowhigh_row():
    inst = Delta_analysis()
    inst.parse(TICK)
    inst.parse(TICK2)
    assert not np.all(np.isnan(inst.lowHigh['buy'][0]))


def test_price_bounds_expand_left():
    inst = Delta_analysis()
    inst.parse(TICK)
    original_base = inst.base_ltp
    low_tick = {**TICK2, 'last_price': str(original_base - 100), 'volume_traded': '300'}
    inst.parse(low_tick)
    assert inst.base_ltp < original_base


def test_price_bounds_expand_right():
    inst = Delta_analysis()
    inst.parse(TICK)
    original_width = inst.WIDTH
    high_tick = {**TICK2, 'last_price': str(int(float(TICK['last_price'])) + inst.WIDTH + 10), 'volume_traded': '300'}
    inst.parse(high_tick)
    assert inst.WIDTH > original_width
