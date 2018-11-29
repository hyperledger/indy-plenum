import functools

from plenum.server.monitor import Monitor
from plenum.test.testing_utils import FakeSomething


def test_replica_not_degraded_with_too_high_latency():
    monitor = FakeSomething(
        is_instance_avg_req_latency_too_high=lambda a: True,
        is_instance_throughput_too_low=lambda a: False,
        acc_monitor=None,
        instances=FakeSomething(backupIds=[1, 2, 3]))
    monitor.areBackupsDegraded = functools.partial(Monitor.areBackupsDegraded, monitor)

    assert not monitor.areBackupsDegraded()
