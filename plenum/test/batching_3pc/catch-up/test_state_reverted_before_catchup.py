import pytest
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.batching_3pc.helper import send_and_check, \
    add_txns_to_ledger_before_order, start_precatchup_before_order
from plenum.test.delayers import cDelay
from plenum.test.helper import signed_random_requests, sendRandomRequests, waitForSufficientRepliesForRequests
from plenum.test.node_catchup.helper import waitNodeDataUnequality
from plenum.test.test_node import getNonPrimaryReplicas

@pytest.fixture(scope="module")
def tconf(tconf, request):
    oldSize = tconf.Max3PCBatchSize
    tconf.Max3PCBatchSize = 10

    def reset():
        tconf.Max3PCBatchSize = oldSize

    request.addfinalizer(reset)
    return tconf

def test_unordered_state_reverted_before_catchup(tconf, looper, txnPoolNodeSet, client,
                            wallet1):
    # CONFIG

    ledger_id = DOMAIN_LEDGER_ID
    non_primary_replica_1 = getNonPrimaryReplicas(txnPoolNodeSet, instId=0)[0]
    non_primary_node_1 = non_primary_replica_1.node

    # send reqs and make sure we are at the same state
    reqs = signed_random_requests(wallet1, tconf.Max3PCBatchSize)
    send_and_check(reqs, looper, txnPoolNodeSet, client)

    # the state of the node before
    committed_ledger_1_before = non_primary_node_1.getLedger(ledger_id).tree.root_hash
    uncommitted_ledger_1_before = non_primary_node_1.getLedger(ledger_id).uncommittedRootHash
    committed_state_1_before = non_primary_node_1.getState(ledger_id).committedHeadHash
    uncommitted_state_1_before = non_primary_node_1.getState(ledger_id).headHash


    # EXECUTE

    # Delay commit requests on the node
    delay_c = 60
    non_primary_node_1.nodeIbStasher.delay(cDelay(delay_c))

    # send requests
    reqs = sendRandomRequests(wallet1, client, tconf.Max3PCBatchSize)
    waitForSufficientRepliesForRequests(looper, client, requests=reqs, total_timeout=40)

    committed_ledger_1_during_3pc = non_primary_node_1.getLedger(ledger_id).tree.root_hash
    uncommitted_ledger_1_during_3pc = non_primary_node_1.getLedger(ledger_id).uncommittedRootHash
    committed_state_1_during_3pc = non_primary_node_1.getState(ledger_id).committedHeadHash
    uncommitted_state_1_during_3pc = non_primary_node_1.getState(ledger_id).headHash

    # start catchup
    non_primary_node_1.ledgerManager.preCatchupClbk(ledger_id)

    committed_ledger_1_reverted = non_primary_node_1.getLedger(ledger_id).tree.root_hash
    uncommitted_ledger_1_reverted = non_primary_node_1.getLedger(ledger_id).uncommittedRootHash
    committed_state_1_reverted = non_primary_node_1.getState(ledger_id).committedHeadHash
    uncommitted_state_1_reverted = non_primary_node_1.getState(ledger_id).headHash

    # CHECK

    # check that initial state differs from the state during 3PC (before revert)
    assert committed_ledger_1_before == committed_ledger_1_during_3pc
    assert uncommitted_ledger_1_before != uncommitted_ledger_1_during_3pc
    assert committed_state_1_before == committed_state_1_during_3pc
    assert uncommitted_state_1_before != uncommitted_state_1_during_3pc

    # check that the state of node 1 is the same as before (that is we reverted the state)
    assert committed_ledger_1_before == committed_ledger_1_reverted
    assert uncommitted_ledger_1_before == uncommitted_ledger_1_reverted
    assert committed_state_1_before == committed_state_1_reverted
    assert uncommitted_state_1_before == uncommitted_state_1_reverted

