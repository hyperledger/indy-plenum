from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.batching_3pc.helper import send_and_check
from plenum.test.delayers import cDelay
from plenum.test.helper import signed_random_requests, sendRandomRequests, waitForSufficientRepliesForRequests
from plenum.test.test_node import getNonPrimaryReplicas


def test_unordered_state_reverted_before_catchup(
        tconf, looper, txnPoolNodeSet, client, wallet1):
    """
    Check that unordered state is reverted before starting catchup:
    - save the initial state on a node
    - slow down processing of COMMITs
    - send requests
    - wait until other nodes come to consensus
    - call start of catch-up
    - check that the state of the slow node is reverted and equal to the initial one.
    """
    # CONFIG

    ledger_id = DOMAIN_LEDGER_ID
    non_primary_node = getNonPrimaryReplicas(txnPoolNodeSet, instId=0)[0].node
    non_primary_ledger = non_primary_node.getLedger(ledger_id)
    non_primary_state = non_primary_node.getState(ledger_id)

    # send reqs and make sure we are at the same state
    reqs = signed_random_requests(wallet1, 10)
    send_and_check(reqs, looper, txnPoolNodeSet, client)

    # the state of the node before
    committed_ledger_before = non_primary_ledger.tree.root_hash
    uncommitted_ledger_before = non_primary_ledger.uncommittedRootHash
    committed_state_before = non_primary_state.committedHeadHash
    uncommitted_state_before = non_primary_state.headHash

    # EXECUTE

    # Delay commit requests on the node
    delay_c = 60
    non_primary_node.nodeIbStasher.delay(cDelay(delay_c))

    # send requests
    reqs = sendRandomRequests(wallet1, client, tconf.Max3PCBatchSize)
    waitForSufficientRepliesForRequests(
        looper, client, requests=reqs, total_timeout=40)

    committed_ledger_during_3pc = non_primary_node.getLedger(
        ledger_id).tree.root_hash
    uncommitted_ledger_during_3pc = non_primary_node.getLedger(
        ledger_id).uncommittedRootHash
    committed_state_during_3pc = non_primary_node.getState(
        ledger_id).committedHeadHash
    uncommitted_state_during_3pc = non_primary_node.getState(
        ledger_id).headHash

    # start catchup
    non_primary_node.ledgerManager.preCatchupClbk(ledger_id)

    committed_ledger_reverted = non_primary_ledger.tree.root_hash
    uncommitted_ledger_reverted = non_primary_ledger.uncommittedRootHash
    committed_state_reverted = non_primary_state.committedHeadHash
    uncommitted_state_reverted = non_primary_state.headHash

    # CHECK

    # check that initial uncommitted state differs from the state during 3PC
    #  but committed does not
    assert committed_ledger_before == committed_ledger_during_3pc
    assert uncommitted_ledger_before != uncommitted_ledger_during_3pc
    assert committed_state_before == committed_state_during_3pc
    assert uncommitted_state_before != uncommitted_state_during_3pc

    assert committed_ledger_before == committed_ledger_reverted
    assert uncommitted_ledger_before == uncommitted_ledger_reverted
    assert committed_state_before == committed_state_reverted
    assert uncommitted_state_before == uncommitted_state_reverted
