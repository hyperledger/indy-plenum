from plenum.test.testing_utils import FakeSomething


def test_reset_monitor_after_view_change(create_node_and_not_start):
    node = create_node_and_not_start
    node.monitor.throughputs[0].throughput = 1
    node.monitor.throughputs[0].first_ts = 0
    node.monitor.throughputs[1].throughput = 100
    node.monitor.throughputs[1].first_ts = 0
    node.replicas._replicas[0]._primaryName = "Alpha:0"
    node.replicas._replicas[1]._primaryName = "Beta:1"
    node.view_changer = FakeSomething(propagate_primary=False,
                                      last_completed_view_no=0,
                                      view_no=1)
    node.on_view_change_complete()
    # After reset throughput must be 0 or None
    # depending on the throughput measurement strategy
    assert not node.monitor.getThroughput(0)
    assert not node.monitor.getThroughput(1)
    assert node.monitor.isMasterDegraded() is False
