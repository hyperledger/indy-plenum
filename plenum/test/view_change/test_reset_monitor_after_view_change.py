from plenum.common.messages.internal_messages import ReAppliedInNewView
from plenum.test.testing_utils import FakeSomething


def test_reset_monitor_after_view_change_events(create_node_and_not_start):
    node = create_node_and_not_start
    node.view_changer = FakeSomething(propagate_primary=False,
                                      view_no=1)

    node.monitor.throughputs[0].throughput = 1
    node.monitor.throughputs[0].first_ts = 0
    node.monitor.throughputs[1].throughput = 100
    node.monitor.throughputs[1].first_ts = 0
    node.replicas._replicas[0].primaryName = "Alpha:0"
    node.replicas._replicas[1].primaryName = "Beta:1"

    # TODO: Actually it would be nice to check that NewViewAccepted also resets monitor,
    #  however this requires either much more mocking (which is fragile) or rewriting test
    #  to use actual Replicas
    node._process_re_applied_in_new_view(ReAppliedInNewView())

    # After reset throughput must be 0 or None
    # depending on the throughput measurement strategy
    assert not node.monitor.getThroughput(0)
    assert not node.monitor.getThroughput(1)
    assert node.monitor.isMasterDegraded() is False
