import copy
import random

import pytest

ACCURACY = 1e-3


@pytest.fixture(scope='function')
def latency_class(tconf):
    return tconf.LatencyMeasurementCls(tconf)


def test_get_avg_latency(latency_class):
    assert latency_class.get_avg_latency


def test_add_duration(latency_class):
    lc = latency_class
    lc.add_duration('some_client_identifier', 1)
    assert lc.get_avg_latency() != 0


def test_avg_latency_accuracy(latency_class):
    count_of_insertion = 1000
    lc = latency_class
    duration = 10
    for _ in range(0, count_of_insertion):
        lc.add_duration('some client identifier', duration)
    avg_lat = lc.get_avg_latency()
    assert abs(avg_lat - duration) < ACCURACY
    total_reqs = lc.total_reqs
    assert total_reqs == count_of_insertion


def test_get_avg_latency_none_if_less_then_needed_count(latency_class, tconf):
    lc = latency_class
    identifier = 'some_client_identifier'
    duration = 10
    for _ in range(tconf.MIN_LATENCY_COUNT - 1):
        lc.add_duration(identifier, duration)
    assert lc.get_avg_latency() is None


def test_avg_latency_accuracy_master_and_backup(latency_class, tconf):
    lc1 = latency_class
    lc2 = copy.deepcopy(latency_class)
    identifier = 'some_client_identifier'
    for i in range(1, tconf.MIN_LATENCY_COUNT + 1):
        duration1 = random.randint(0, tconf.OMEGA)
        lc1.add_duration(identifier, duration1)

        duration2 = random.randint(0, tconf.OMEGA)
        lc2.add_duration(identifier, duration2)

    assert abs(lc1.get_avg_latency() - lc2.get_avg_latency()) < tconf.OMEGA
