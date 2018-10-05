from plenum.common.monitor_strategies import AccumulatingMonitorStrategy

ACC_MONITOR_TXN_DELTA_K = 100
ACC_MONITOR_TIMEOUT = 300.0
ACC_MONITOR_INPUT_RATE_REACTION_HALF_TIME = 300.0

START_TIME = 100.0


def createMonitor():
    return AccumulatingMonitorStrategy(start_time=START_TIME,
                                       instances=set(range(2)),
                                       txn_delta_k=ACC_MONITOR_TXN_DELTA_K,
                                       timeout=ACC_MONITOR_TIMEOUT,
                                       input_rate_reaction_half_time=ACC_MONITOR_INPUT_RATE_REACTION_HALF_TIME)


def test_acc_monitor_isnt_alerted_when_created():
    mon = createMonitor()
    assert not mon.is_master_degraded()


# TODO: This is just quick'n'dirty smoke test, there should be much more tested
def test_acc_monitor_behaves_as_expected():
    mon = createMonitor()

    # Simulate txns receiving and ordering
    start_time = 100.0
    mon.update_time(start_time)
    mon.request_received('A')
    mon.request_received('B')
    mon.request_received('C')
    mon.request_received('D')

    mon.update_time(start_time + 0.4 * ACC_MONITOR_TIMEOUT)
    mon.request_ordered('A', 1)
    mon.request_ordered('B', 1)
    mon.request_ordered('C', 1)
    mon.request_ordered('D', 1)

    # Give monitor chance to observe txn statistics
    triggered_time = start_time + 0.6 * ACC_MONITOR_TIMEOUT
    mon.update_time(triggered_time)

    # Monitor should not be triggered before timeout
    mon.update_time(triggered_time + 0.8 * ACC_MONITOR_TIMEOUT)
    assert not mon.is_master_degraded()

    # Monitor should be triggered after timeout
    mon.update_time(triggered_time + 1.2 * ACC_MONITOR_TIMEOUT)
    assert mon.is_master_degraded()

    # Monitor should not be triggered after reset
    mon.reset()
    assert not mon.is_master_degraded()

    # Give monitor chance to observe txn statistics
    reset_time = triggered_time + 2.4 * ACC_MONITOR_TIMEOUT
    mon.update_time(reset_time)

    # Monitor should not be triggered if nothing is ordered
    mon.update_time(reset_time + 1.2 * ACC_MONITOR_TIMEOUT)
    assert not mon.is_master_degraded()


def test_acc_monitor_works_even_with_one_instance():
    mon = createMonitor()
    mon.remove_instance(1)

    start_time = 100.0
    mon.update_time(start_time)
    mon.request_received('A')
    mon.update_time(start_time + 1.2 * ACC_MONITOR_TIMEOUT)
    assert not mon.is_master_degraded()

    mon.request_ordered('A', 0)
    mon.update_time(start_time + 2.4 * ACC_MONITOR_TIMEOUT)
    assert not mon.is_master_degraded()
