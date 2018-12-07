import pytest
from plenum.common.monitor_strategies import AccumulatingMonitorStrategy

ACC_MONITOR_TXN_DELTA_K = 100
ACC_MONITOR_TIMEOUT = 300.0
ACC_MONITOR_INPUT_RATE_REACTION_HALF_TIME = 300.0

START_TIME = 100.0


@pytest.fixture()
def monitor():
    return AccumulatingMonitorStrategy(start_time=START_TIME,
                                       instances=set(range(2)),
                                       txn_delta_k=ACC_MONITOR_TXN_DELTA_K,
                                       timeout=ACC_MONITOR_TIMEOUT,
                                       input_rate_reaction_half_time=ACC_MONITOR_INPUT_RATE_REACTION_HALF_TIME)


def simulate_ordering(monitor, start_time, inst_id):
    monitor.update_time(start_time)
    monitor.request_received('A')
    monitor.request_received('B')
    monitor.request_received('C')
    monitor.request_received('D')

    monitor.update_time(start_time + 0.4 * ACC_MONITOR_TIMEOUT)
    monitor.request_ordered('A', inst_id)
    monitor.request_ordered('B', inst_id)
    monitor.request_ordered('C', inst_id)
    monitor.request_ordered('D', inst_id)

    # Give monitor chance to observe txn statistics
    triggered_time = start_time + 0.6 * ACC_MONITOR_TIMEOUT
    monitor.update_time(triggered_time)
    return triggered_time


def test_acc_monitor_isnt_alerted_when_created(monitor: AccumulatingMonitorStrategy):
    assert not monitor.is_master_degraded()
    assert not monitor.is_instance_degraded(1)


# TODO: This is just quick'n'dirty smoke test, there should be much more tested
def test_acc_monitor_behaves_as_expected(monitor: AccumulatingMonitorStrategy):
    # Simulate txns receiving and ordering
    triggered_time = simulate_ordering(monitor, 100.0, 1)

    # Monitor should not be triggered before timeout
    monitor.update_time(triggered_time + 0.8 * ACC_MONITOR_TIMEOUT)
    assert not monitor.is_master_degraded()
    assert not monitor.is_instance_degraded(1)

    # Monitor should be triggered on master after timeout
    monitor.update_time(triggered_time + 1.2 * ACC_MONITOR_TIMEOUT)
    assert monitor.is_master_degraded()
    assert not monitor.is_instance_degraded(1)

    # Monitor should not be triggered after reset
    monitor.reset()
    assert not monitor.is_master_degraded()
    assert not monitor.is_instance_degraded(1)

    # Give monitor chance to observe txn statistics
    reset_time = triggered_time + 2.4 * ACC_MONITOR_TIMEOUT
    monitor.update_time(reset_time)

    # Monitor should not be triggered if nothing is ordered
    monitor.update_time(reset_time + 1.2 * ACC_MONITOR_TIMEOUT)
    assert not monitor.is_master_degraded()
    assert not monitor.is_instance_degraded(1)


def test_acc_monitor_detects_backup_slowdown(monitor: AccumulatingMonitorStrategy):
    # Simulate txns receiving and ordering
    triggered_time = simulate_ordering(monitor, 100.0, 0)

    # Monitor should not be triggered before timeout
    monitor.update_time(triggered_time + 0.8 * ACC_MONITOR_TIMEOUT)
    assert not monitor.is_master_degraded()
    assert not monitor.is_instance_degraded(1)

    # Monitor should be triggered on backup after timeout
    monitor.update_time(triggered_time + 1.2 * ACC_MONITOR_TIMEOUT)
    assert not monitor.is_master_degraded()
    assert monitor.is_instance_degraded(1)


def test_acc_monitor_works_even_with_one_instance(monitor: AccumulatingMonitorStrategy):
    monitor.remove_instance(1)

    start_time = 100.0
    monitor.update_time(start_time)
    monitor.request_received('A')
    monitor.update_time(start_time + 1.2 * ACC_MONITOR_TIMEOUT)
    assert not monitor.is_master_degraded()

    monitor.request_ordered('A', 0)
    monitor.update_time(start_time + 2.4 * ACC_MONITOR_TIMEOUT)
    assert not monitor.is_master_degraded()


def test_acc_monitor_handles_adding_instances(monitor: AccumulatingMonitorStrategy):
    monitor.add_instance(2)

    triggered_time = simulate_ordering(monitor, 100.0, 2)

    # Monitor should not be triggered before timeout
    monitor.update_time(triggered_time + 0.8 * ACC_MONITOR_TIMEOUT)
    assert not monitor.is_master_degraded()
    assert not monitor.is_instance_degraded(1)
    assert not monitor.is_instance_degraded(2)

    # Monitor should be triggered after timeout
    monitor.update_time(triggered_time + 1.2 * ACC_MONITOR_TIMEOUT)
    assert monitor.is_master_degraded()
    assert monitor.is_instance_degraded(1)  # TODO: Monitor behaves like this now, but is it okay?
    assert not monitor.is_instance_degraded(2)
