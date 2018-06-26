import json

import pytest

from plenum.recorder.replayer import _cal_run_times


def test_cal_run_times():
    start_times1 = """[
    [1528997177,1528997752],
    [1528997798,1528997866],
    [1528997909,1528997981],
    [1528998021,1528998091],
    [1528998133,1528999133]
    ]"""
    start_times1 = json.loads(start_times1)

    with pytest.raises(Exception):
        _cal_run_times(0, start_times1)

    s, a = _cal_run_times(1, start_times1)
    assert s == 46
    assert a == 68

    s, a = _cal_run_times(4, start_times1)
    assert s == 42
    assert a == 1000

    with pytest.raises(Exception):
        s, a = _cal_run_times(10, start_times1)

    start_times2 = """[
    [1528997177,1528997752],
    [1528998133]
    ]"""
    start_times2 = json.loads(start_times2)
    s, a = _cal_run_times(1, start_times2)
    assert s == 381
    assert a is None
