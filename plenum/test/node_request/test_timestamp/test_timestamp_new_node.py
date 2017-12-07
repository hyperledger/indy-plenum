from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.node_request.test_timestamp.helper import \
    get_timestamp_suspicion_count
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change
from plenum.test.helper import sdk_send_random_and_check

txnCount = 20
Max3PCBatchSize = 4

from plenum.test.node_catchup.conftest import nodeCreatedAfterSomeTxns, \
    nodeSetWithNodeAddedAfterSomeTxns
from plenum.test.batching_3pc.conftest import tconf

# lot of requests will be sent
TestRunningTimeLimitSec = 200


def test_new_node_accepts_timestamp(tconf, looper, txnPoolNodeSet,
                                    nodeSetWithNodeAddedAfterSomeTxns,
                                    sdk_wallet_client, sdk_pool_handle):
    """
    A new node joins the pool and is able to function properly without
    """
    _, new_node, _, _, _, _ = nodeSetWithNodeAddedAfterSomeTxns
    old_susp_count = get_timestamp_suspicion_count(new_node)
    # Don't wait for node to catchup, start sending requests
    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              count=10)
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1])

    # No suspicions were raised by new_node
    assert get_timestamp_suspicion_count(new_node) == old_susp_count

    # All nodes should reply
    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              count=Max3PCBatchSize * 3)
    # No suspicions were raised by new_node
    assert get_timestamp_suspicion_count(new_node) == old_susp_count

    suspicions = {node.name: get_timestamp_suspicion_count(
        node) for node in txnPoolNodeSet}
    ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              count=Max3PCBatchSize * 3)
    for node in txnPoolNodeSet:
        assert suspicions[node.name] == get_timestamp_suspicion_count(node)
