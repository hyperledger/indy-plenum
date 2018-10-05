import pytest
import time

from plenum.common.throughput_measurements import SafeStartEMAThroughputMeasurement

ACCURACY = .1e-3


@pytest.fixture(scope="function")
def request_measurement():
    rm = SafeStartEMAThroughputMeasurement()
    rm.init_time(1)
    return rm


def test_add_request(request_measurement):
    assert request_measurement.add_request


@pytest.mark.skip(reason="Not implemented yet")
def test_get_max_latency(request_measurement):
    assert request_measurement.get_max_latency


def test_get_throughput(request_measurement):
    assert request_measurement.get_throughput


@pytest.mark.skip(reason="Not implemented yet")
def test_avg_latency(rm_with_random_requests, recv_ordered_ts):
    assert abs(rm_with_random_requests.get_avg_latency() - sum([b - a for a, b in recv_ordered_ts]) / len(recv_ordered_ts)) < ACCURACY


@pytest.mark.skip(reason="Not implemented yet")
def test_max_latency(rm_with_random_requests, recv_ordered_ts):
    assert rm_with_random_requests.get_max_latency() == max([b - a for a, b in recv_ordered_ts])


def test_add_request_and_eval_first_throughput(request_measurement):
    rm = request_measurement
    ordered_ts = [x for x in range(1, rm.window_size + 2)]
    assert len(ordered_ts) == rm.window_size + 1
    for ts in ordered_ts:
        request_measurement.add_request(ordered_ts=ts)
    assert rm.throughput > 0
    assert rm.window_start_ts == rm.first_ts + rm.window_size


def test_get_thoughput_ts_less_than_window(request_measurement):
    rm = request_measurement
    ordered_ts = [1, 2, 3, 5, rm.min_cnt * rm.window_size]
    for ts in ordered_ts:
        rm.add_request(ordered_ts=ts)
    assert rm.get_throughput(rm.min_cnt * rm.window_size) is None


def test_get_throughput_out_of_first_window(request_measurement):
    rm = request_measurement
    for ts in [1, 2, 3, rm.window_start_ts + rm.window_size + 1]:
        rm.add_request(ordered_ts=ts)
    assert rm.window_start_ts == rm.first_ts + rm.window_size
    assert rm.reqs_in_window == 1


def test_get_throughput_meaning_avg_accuracy(request_measurement):
    """
    Add 10 * min_cnt * window_size = 2400 of consistent timestamps.
    Asymptotically, throughput in that case must be around 1 (2400 ordered for 2400 seconds)
    """
    rm = request_measurement
    request_ts = 10 * rm.min_cnt * rm.window_size
    for ts in range(1, request_ts + 1):
        rm.add_request(ordered_ts=ts)
    throughput = rm.get_throughput(request_ts)
    assert abs(throughput - 1) < ACCURACY


def test_get_throughput_return_not_none_if_greater_that_threshold(request_measurement):
    rm = request_measurement
    ordered_ts = [1, 2, 3, 5, 240, 240, 240, rm.min_cnt * rm.window_size]
    for ts in ordered_ts:
        rm.add_request(ordered_ts=ts)
    assert rm.get_throughput(rm.min_cnt * rm.window_size + 1) > 0


def test_get_throughput_return_if_ts_only_for_first_window(request_measurement):
    rm = request_measurement
    ordered_ts = [x for x in range(1, rm.window_size)]
    for ts in ordered_ts:
        rm.add_request(ordered_ts=ts)
    # All of timestamps are included into first window
    assert rm.first_ts == rm.window_start_ts
    assert rm.get_throughput(rm.min_cnt * rm.window_size + 10) is not None


def test_get_throughput_return_0_if_there_is_no_any_requested(request_measurement):
    rm = request_measurement
    assert rm.get_throughput(time.perf_counter()) == 0


def test_update_time(request_measurement):
    rm = request_measurement
    ordered_ts = [x for x in range(rm.window_size + 2)]
    # Check, that last elem is out from first window
    assert ordered_ts[-1] > rm.window_size
    for ts in ordered_ts:
        rm.add_request(ordered_ts=ts)
    # Check, that window was moved
    assert rm.first_ts != rm.window_start_ts
    # Check that now in new window we have exactly 1 ordered timestamp
    assert rm.reqs_in_window == 1
    # Check, that throughput for first window was calculated
    assert rm.throughput > 0
