import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def test_delta_graph_importable():
    import live.delta_graph  # noqa: F401


def test_delta_graph_exposes_function():
    from live import delta_graph
    assert callable(delta_graph.delta_graph)


def test_pos_pct_matches_mark_stats_semantics():
    import numpy as np
    from live.delta_graph import _pos_pct

    arr = np.array([
        [1.0, -1.0, np.nan],
        [0.0,  2.0, -3.0],
    ])
    # tick 0: 1 of 2 valid cells >= 0 -> 50%
    # tick 1: 3 of 5 cumulative valid cells >= 0 -> 60%
    assert np.allclose(_pos_pct(arr), [50.0, 60.0])
    assert np.all(np.isnan(_pos_pct(np.full((2, 2), np.nan))))


def test_pos_pct_rolling_window_drops_the_prefix():
    import numpy as np
    from live.delta_graph import _pos_pct

    arr = np.array([
        [1.0,  2.0],    # all positive
        [1.0,  2.0],    # all positive
        [-1.0, -2.0],   # all negative
    ])
    # window=1 looks at the current tick only: 100, 100, 0
    assert np.allclose(_pos_pct(arr, window=1), [100.0, 100.0, 0.0])
    # window=2 over ticks 1..2: 2 of 4 valid cells >= 0 -> 50%
    assert np.allclose(_pos_pct(arr, window=2), [100.0, 100.0, 50.0])
    # a window at least as long as the data equals the cumulative result
    assert np.allclose(_pos_pct(arr, window=3), _pos_pct(arr))


def test_bias_ribbon_rgba_colours_by_sign():
    import numpy as np
    from live.delta_graph import _bias_ribbon_rgba

    rgba = _bias_ribbon_rgba(np.array([-80.0, 80.0, np.nan]))
    assert tuple(rgba[0, 0, :3]) == (200, 0, 0)    # negative -> red
    assert tuple(rgba[1, 0, :3]) == (0, 150, 0)    # positive -> green
    assert rgba[2, 0, 3] == 0                      # NaN -> transparent
    assert rgba[0, 0, 3] == 255                    # |bias| past the scale -> opaque
    # pyqtgraph draws nothing for a 1-row ImageItem, so the ribbon is 2 rows deep
    assert rgba.shape == (3, 2, 4)
    assert np.array_equal(rgba[:, 0, :], rgba[:, 1, :])
