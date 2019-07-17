from plenum.test.pool_transactions.helper import sdk_add_new_nym_without_waiting
from stp_core.loop.eventually import eventually

from plenum.test.delayers import ppDelay, cDelay, pDelay

from plenum.test.stasher import delay_rules


def test_check_cdp_pp_storages(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward):
    def check_all_empty(replica, reverse=False):
        check_preprepared_empty(replica, reverse)
        check_prepared_empty(replica, reverse)

    def check_preprepared_empty(replica, reverse=False):
        statement_pp = bool(replica._consensus_data.preprepared)
        statement_pp ^= reverse
        assert not statement_pp

    def check_prepared_empty(replica, reverse=False):
        statement_p = bool(replica._consensus_data.prepared)
        statement_p ^= reverse
        assert not statement_p

    def operation_for_replicas(operation, node_set=txnPoolNodeSet, reverse=False):
        for node in node_set:
            operation(node.master_replica, reverse)

    node_stashers = [n.nodeIbStasher for n in txnPoolNodeSet]

    with delay_rules(node_stashers, cDelay()):
        with delay_rules(node_stashers, pDelay()):
            with delay_rules(node_stashers, ppDelay()):
                sdk_add_new_nym_without_waiting(looper, sdk_pool_handle, sdk_wallet_steward)
                looper.run(eventually(operation_for_replicas, check_all_empty, txnPoolNodeSet[1:]))
                looper.run(eventually(operation_for_replicas, check_preprepared_empty, txnPoolNodeSet[0:1], True))
            looper.run(eventually(operation_for_replicas, check_preprepared_empty, txnPoolNodeSet, True))
        looper.run(eventually(operation_for_replicas, check_prepared_empty, txnPoolNodeSet, True))
    looper.run(eventually(operation_for_replicas, check_all_empty, txnPoolNodeSet, True))
