from plenum.server.monitor import AccumulatingMonitorStrategy

MON_THRESHOLD = 2
MON_TIMEOUT = 300.0

def createMonitor():
    return AccumulatingMonitorStrategy(instances=2, threshold=MON_THRESHOLD, timeout=MON_TIMEOUT)


def test_acc_monitor_isnt_alerted_when_created():
    mon = createMonitor()
    assert not mon.is_master_degraded()


# TODO: This is just quick'n'dirty smoke test, there should be much more tested
def test_acc_monitor_behaves_as_expected():
    mon = createMonitor()

    mon.update_time(100.0)
    mon.request_received('A')
    mon.request_received('B')
    mon.request_received('C')
    mon.request_received('D')

    mon.update_time(150.0)
    mon.request_ordered('A', 1)
    mon.request_ordered('B', 1)
    mon.request_ordered('C', 1)
    mon.request_ordered('D', 1)

    mon.update_time(200.0)
    assert not mon.is_master_degraded()

    mon.update_time(1000.0)
    assert mon.is_master_degraded()

    mon.reset()
    assert not mon.is_master_degraded()

    mon.update_time(1100.0)
    assert not mon.is_master_degraded()
