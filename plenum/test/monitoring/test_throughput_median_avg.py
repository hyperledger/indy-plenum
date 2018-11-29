def test_low_median(fake_monitor, tconf):
    monitor = fake_monitor
    # Filling for case, when the most of backup throughputs are small and several is very big
    # Case is [100, 120, 120, 120, 10000]
    for i in monitor.instances.ids:
        monitor.throughputs[i].throughput = 120
    monitor.throughputs[0].throughput = 100
    monitor.throughputs[monitor.instances.count - 1].throughput = 10000
    assert monitor.instance_throughput_ratio(0) > tconf.DELTA


def test_medium_median(fake_monitor, tconf):
    monitor = fake_monitor
    # Filling for case, when the most of backup throughputs a similar with master,
    # but there is a very small and a very big
    # Case is [100, 1, 120, 120, 10000]
    for i in monitor.instances.ids:
        monitor.throughputs[i].throughput = 120
    monitor.throughputs[0].throughput = 100
    monitor.throughputs[1].throughput = 1
    monitor.throughputs[monitor.instances.count - 1].throughput = 10000
    assert monitor.instance_throughput_ratio(0) > tconf.DELTA


def test_high_median(fake_monitor, tconf):
    monitor = fake_monitor
    # Filling for case, when the most of backup throughputs a similar with master,
    # but there is a some very big values
    # Case is [100, 1, 120, 120, 1]
    for i in monitor.instances.ids:
        monitor.throughputs[i].throughput = 120
    monitor.throughputs[0].throughput = 100
    monitor.throughputs[1].throughput = 1
    monitor.throughputs[monitor.instances.count - 1].throughput = 1
    assert monitor.instance_throughput_ratio(0) > tconf.DELTA


def test_triggering_view_change(fake_monitor, tconf):
    monitor = fake_monitor
    # Filling for case, when the all of backup throughputs are higher, then for master
    for i in monitor.instances.ids:
        monitor.throughputs[i].throughput = 1001
    monitor.throughputs[0].throughput = 100
    assert monitor.instance_throughput_ratio(0) < tconf.DELTA
