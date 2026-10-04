import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def test_delta_graph_importable():
    import live.delta_graph  # noqa: F401


def test_delta_graph_exposes_function():
    from live import delta_graph
    assert callable(delta_graph.delta_graph)


def test_cumulative_pos_pct_matches_mark_stats_semantics():
    import numpy as np
    from live.delta_graph import _cumulative_pos_pct

    arr = np.array([
        [1.0, -1.0, np.nan],
        [0.0,  2.0, -3.0],
    ])
    # tick 0: 1 of 2 valid cells >= 0 -> 50%
    # tick 1: 3 of 5 cumulative valid cells >= 0 -> 60%
    assert np.allclose(_cumulative_pos_pct(arr), [50.0, 60.0])
    assert np.all(np.isnan(_cumulative_pos_pct(np.full((2, 2), np.nan))))
