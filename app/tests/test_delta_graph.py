import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def test_delta_graph_importable():
    import live.delta_graph  # noqa: F401


def test_delta_graph_exposes_function():
    from live import delta_graph
    assert callable(delta_graph.delta_graph)
