from plenum.test.pool_transactions.helper import sdk_add_new_nym_without_waiting
from stp_core.loop.eventually import eventually

from plenum.test.delayers import ppDelay, pDelay

from plenum.test.stasher import delay_rules


def test_check_cdp_pp_storages(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward):
    def check_all_empty(replica):
        assert not bool(replica._consensus_data.preprepared)
        assert not bool(replica._consensus_data.prepared)

    def check_preprepared_not_empty(replica):
        assert bool(replica._consensus_data.preprepared)

    def check_prepared_not_empty(replica):
        assert bool(replica._consensus_data.prepared)

    def operation_for_replicas(operation, node_set=txnPoolNodeSet):
        for node in node_set:
            operation(node.master_replica)

    node_stashers = [n.nodeIbStasher for n in txnPoolNodeSet]

    with delay_rules(node_stashers, pDelay()):
        with delay_rules(node_stashers, ppDelay()):
            sdk_add_new_nym_without_waiting(looper, sdk_pool_handle, sdk_wallet_steward)
            looper.run(eventually(operation_for_replicas, check_all_empty, txnPoolNodeSet[1:]))
            looper.run(eventually(operation_for_replicas, check_preprepared_not_empty, txnPoolNodeSet[0:1]))
        looper.run(eventually(operation_for_replicas, check_preprepared_not_empty, txnPoolNodeSet))
    looper.run(eventually(operation_for_replicas, check_prepared_not_empty, txnPoolNodeSet))
