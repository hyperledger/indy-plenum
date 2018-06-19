import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.startable import Mode
from plenum.test.delayers import cDelay
from plenum.test.helper import sdk_get_and_check_replies, sdk_send_random_requests
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules
from plenum.test.test_node import getNonPrimaryReplicas
from stp_core.loop.eventually import eventually

TOTAL_REQUESTS = 10


@pytest.fixture(scope="module")
def tconf(tconf):
    old_max_batch_wait = tconf.Max3PCBatchWait
    old_max_batch_size = tconf.Max3PCBatchSize
    # Make sure that all requests in test will end up in one batch
    tconf.Max3PCBatchWait = 1000
    tconf.Max3PCBatchSize = TOTAL_REQUESTS
    yield tconf
    tconf.Max3PCBatchWait = old_max_batch_wait
    tconf.Max3PCBatchSize = old_max_batch_size


def test_apply_stashed_partially_ordered(looper,
                                         txnPoolNodeSet,
                                         sdk_pool_handle,
                                         sdk_wallet_client):
    test_node = getNonPrimaryReplicas(txnPoolNodeSet)[0].node
    test_stasher = test_node.nodeIbStasher
    ledger_size = max(node.domainLedger.size for node in txnPoolNodeSet)

    def check_pool_ordered_some_requests():
        assert max(node.domainLedger.size for node in txnPoolNodeSet) > ledger_size

    def check_test_node_has_stashed_ordered_requests():
        assert len(test_node.stashedOrderedReqs) > 0

    # Delay COMMITs so requests are not ordered on test node
    with delay_rules(test_stasher, cDelay()):
        reqs = sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, TOTAL_REQUESTS)
        looper.run(eventually(check_pool_ordered_some_requests))

    # Get some of txns that need to be ordered
    ledger_info = test_node.ledgerManager.getLedgerInfoByType(DOMAIN_LEDGER_ID)
    txns = ledger_info.ledger.uncommittedTxns
    txns = txns[:len(txns) // 2]
    assert len(txns) > 1

    # Emulate incomplete catchup simultaneous with generation of ORDERED message
    test_node.mode = Mode.syncing
    test_node.master_replica.revert_unordered_batches()
    looper.run(eventually(check_test_node_has_stashed_ordered_requests))
    for txn in txns:
        ledger_info.ledger.add(txn)
        ledger_info.postTxnAddedToLedgerClbk(DOMAIN_LEDGER_ID, txn)
    test_node.mode = Mode.participating
    test_node.processStashedOrderedReqs()

    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    sdk_get_and_check_replies(looper, reqs)
