import copy

import pytest

from plenum.common.latency_measurements import EMALatencyMeasurementForEachClient


@pytest.fixture(scope='function')
def latency_instance(tconf):
    lm = EMALatencyMeasurementForEachClient(tconf)
    return lm


def fill_durations(li, client):
    for idf, durations in client.items():
        durations = client[idf]
        for d in durations:
            li.add_duration(idf, d)


def test_spike_from_one_client_independ_of_order(latency_instance, tconf):
    liM = latency_instance
    liB = copy.deepcopy(latency_instance)
    master_lat = 100
    num_reqs = 100
    client1 = {"first_client": [master_lat] * num_reqs}
    client2 = {"second_client": [master_lat / 10] * (int(num_reqs / 10))}
    fill_durations(liM, client1)

    fill_durations(liB, client1)
    fill_durations(liB, client2)

    assert liM.get_avg_latency() - liB.get_avg_latency() < tconf.OMEGA


def test_spike_from_one_request_from_one_client(latency_instance, tconf):
    liM = latency_instance
    liB = copy.deepcopy(latency_instance)
    master_lat = 100
    num_reqs = 100
    client1 = {"first_client": [master_lat] * num_reqs}
    client2 = {"second_client": [master_lat / 10]}
    fill_durations(liM, client1)

    fill_durations(liB, client1)
    fill_durations(liB, client2)
    assert liM.get_avg_latency() - liB.get_avg_latency() < tconf.OMEGA


def test_spike_from_one_request_on_one_client(latency_instance, tconf):
    liM = latency_instance
    liB = copy.deepcopy(latency_instance)
    master_lat = 100
    num_reqs = 100
    clientM = {"master_client": [master_lat] * num_reqs}
    clientB = {"backup_client": [master_lat] * num_reqs + [master_lat / 10]}
    fill_durations(liM, clientM)
    fill_durations(liB, clientB)

    assert liM.get_avg_latency() - liB.get_avg_latency() < tconf.OMEGA


def test_spike_on_two_clients_on_backup(latency_instance, tconf):
    liM = latency_instance
    liB = copy.deepcopy(latency_instance)
    master_lat = 100
    num_reqs = 100
    clientM1 = {"master_client1": [master_lat] * num_reqs}
    clientM2 = {"master_client2": [master_lat / 10] * num_reqs}
    clientB1 = {"backup_client1": [master_lat] * num_reqs + [master_lat / 10]}
    clientB2 = {"backup_client2": [master_lat / 10] * num_reqs + [master_lat]}
    fill_durations(liM, clientM1)
    fill_durations(liM, clientM2)
    fill_durations(liB, clientB1)
    fill_durations(liB, clientB2)

    assert liM.get_avg_latency() - liB.get_avg_latency() < tconf.OMEGA


def test_latency_too_high(latency_instance, tconf):
    liM = latency_instance
    liB = copy.deepcopy(latency_instance)
    master_lat = 100
    num_reqs = 50
    clientM = {"master_client": [master_lat] * num_reqs}
    clientB = {"backup_client": [master_lat / 2] * num_reqs}
    fill_durations(liM, clientM)
    fill_durations(liB, clientB)

    assert liM.get_avg_latency() - liB.get_avg_latency() > tconf.OMEGA
