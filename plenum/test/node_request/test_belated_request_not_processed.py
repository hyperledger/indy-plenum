from plenum.test import waits
from plenum.test.delayers import cDelay, req_delay, ppgDelay
from plenum.test.helper import sdk_signed_random_requests, \
    sdk_send_signed_requests, sdk_send_and_check
from plenum.test.pool_transactions.conftest import looper
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change

from stp_core.common.log import getlogger
from plenum.common.util import updateNamedTuple
from plenum.common.messages.node_messages import PrePrepare
from plenum.server.suspicion_codes import Suspicions

logger = getlogger()

def test_repeated_request_not_processed_if_already_ordered(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):

    delta = txnPoolNodeSet[3]
    initial_ledger_size = delta.domainLedger.size

    one_req = sdk_signed_random_requests(looper, sdk_wallet_client, 1)
    sdk_send_and_check(one_req, looper, txnPoolNodeSet, sdk_pool_handle)

    sdk_send_signed_requests(sdk_pool_handle, one_req)
    looper.runFor(waits.expectedTransactionExecutionTime(len(txnPoolNodeSet)))

    for node in txnPoolNodeSet:
        assert node.domainLedger.size - initial_ledger_size == 1


def test_belated_request_not_processed_if_already_ordered(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):

    delta = txnPoolNodeSet[3]
    initial_ledger_size = delta.domainLedger.size
    delta.clientIbStasher.delay(req_delay(300))

    one_req = sdk_signed_random_requests(looper, sdk_wallet_client, 1)
    sdk_send_and_check(one_req, looper, txnPoolNodeSet, sdk_pool_handle)

    delta.clientIbStasher.reset_delays_and_process_delayeds()
    looper.runFor(waits.expectedTransactionExecutionTime(len(txnPoolNodeSet)))

    for node in txnPoolNodeSet:
        assert node.domainLedger.size - initial_ledger_size == 1


def test_belated_propagate_not_processed_if_already_ordered(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):

    delta = txnPoolNodeSet[3]
    initial_ledger_size = delta.domainLedger.size
    delta.nodeIbStasher.delay(ppgDelay(300, 'Gamma'))

    one_req = sdk_signed_random_requests(looper, sdk_wallet_client, 1)
    sdk_send_and_check(one_req, looper, txnPoolNodeSet, sdk_pool_handle)

    delta.nodeIbStasher.reset_delays_and_process_delayeds()
    looper.runFor(waits.expectedTransactionExecutionTime(len(txnPoolNodeSet)))

    for node in txnPoolNodeSet:
        assert node.domainLedger.size - initial_ledger_size == 1


def test_preprepare_not_processed_if_any_request_is_already_ordered(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):

    nonPrNode = None
    for node in txnPoolNodeSet:
        if not node.has_master_primary:
            break

    nonPrNode = node

    initial_ledger_size = nonPrNode.domainLedger.size

    one_req = sdk_signed_random_requests(looper, sdk_wallet_client, 1)
    sdk_send_and_check(one_req, looper, txnPoolNodeSet, sdk_pool_handle)

    for node in txnPoolNodeSet:
        assert node.domainLedger.size - initial_ledger_size == 1

    nonPrNode_m_replica = nonPrNode.master_replica
    params = nonPrNode_m_replica.spylog.getLastParams(nonPrNode_m_replica.processPrePrepare)
    assert params is not None
    ppPrevious = params["pre_prepare"]
    sender = params['sender']

    # to make really correct (acceptable by all other checks) PP we need update
    # ppTime, and root hashes for state and txn but we don't actually need that here
    ppWithDuplicateIdr = updateNamedTuple(ppPrevious, ppSeqNo=ppPrevious.ppSeqNo + 1)

    count = nonPrNode.spylog.count(nonPrNode.reportSuspiciousNode)
    nonPrNode_m_replica.processPrePrepare(ppWithDuplicateIdr, sender)
    assert nonPrNode.spylog.count(nonPrNode.reportSuspiciousNode) == count + 1

    params = nonPrNode.spylog.getLastParams(nonPrNode.reportSuspiciousNode)
    assert params is not None
    assert params['nodeName'] == nonPrNode_m_replica.getNodeName(sender)
    assert params['reason'] == Suspicions.PPR_INCLUDES_COMMITTED_REQUEST.reason
    assert params['code'] == Suspicions.PPR_INCLUDES_COMMITTED_REQUEST.code
    assert params['offendingMsg'].viewNo == ppWithDuplicateIdr.viewNo
    assert params['offendingMsg'].ppSeqNo == ppWithDuplicateIdr.ppSeqNo


def test_repeated_request_not_processed_if_already_in_3pc_process(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):

    delta = txnPoolNodeSet[3]
    initial_ledger_size = delta.domainLedger.size
    for node in txnPoolNodeSet:
        node.nodeIbStasher.delay(cDelay(300))

    one_req = sdk_signed_random_requests(looper, sdk_wallet_client, 1)
    sdk_send_signed_requests(sdk_pool_handle, one_req)
    looper.runFor(waits.expectedPropagateTime(len(txnPoolNodeSet)) +
                  waits.expectedPrePrepareTime(len(txnPoolNodeSet)) +
                  waits.expectedPrepareTime(len(txnPoolNodeSet)) +
                  waits.expectedCommittedTime(len(txnPoolNodeSet)))

    sdk_send_signed_requests(sdk_pool_handle, one_req)
    looper.runFor(waits.expectedPropagateTime(len(txnPoolNodeSet)) +
                  waits.expectedPrePrepareTime(len(txnPoolNodeSet)) +
                  waits.expectedPrepareTime(len(txnPoolNodeSet)) +
                  waits.expectedCommittedTime(len(txnPoolNodeSet)))

    for node in txnPoolNodeSet:
        node.nodeIbStasher.reset_delays_and_process_delayeds()
    looper.runFor(waits.expectedOrderingTime(delta.replicas.num_replicas))

    for node in txnPoolNodeSet:
        assert node.domainLedger.size - initial_ledger_size == 1


def test_belated_request_not_processed_if_already_in_3pc_process(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):

    delta = txnPoolNodeSet[3]
    initial_ledger_size = delta.domainLedger.size
    delta.clientIbStasher.delay(req_delay(300))
    for node in txnPoolNodeSet:
        node.nodeIbStasher.delay(cDelay(300))

    one_req = sdk_signed_random_requests(looper, sdk_wallet_client, 1)
    sdk_send_signed_requests(sdk_pool_handle, one_req)
    looper.runFor(waits.expectedPropagateTime(len(txnPoolNodeSet)) +
                  waits.expectedPrePrepareTime(len(txnPoolNodeSet)) +
                  waits.expectedPrepareTime(len(txnPoolNodeSet)) +
                  waits.expectedCommittedTime(len(txnPoolNodeSet)))

    delta.clientIbStasher.reset_delays_and_process_delayeds()
    looper.runFor(waits.expectedPropagateTime(len(txnPoolNodeSet)) +
                  waits.expectedPrePrepareTime(len(txnPoolNodeSet)) +
                  waits.expectedPrepareTime(len(txnPoolNodeSet)) +
                  waits.expectedCommittedTime(len(txnPoolNodeSet)))

    for node in txnPoolNodeSet:
        node.nodeIbStasher.reset_delays_and_process_delayeds()
    looper.runFor(waits.expectedOrderingTime(delta.replicas.num_replicas))

    for node in txnPoolNodeSet:
        assert node.domainLedger.size - initial_ledger_size == 1


def test_belated_propagate_not_processed_if_already_in_3pc_process(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):

    delta = txnPoolNodeSet[3]
    initial_ledger_size = delta.domainLedger.size
    delta.nodeIbStasher.delay(ppgDelay(300, 'Gamma'))
    for node in txnPoolNodeSet:
        node.nodeIbStasher.delay(cDelay(300))

    one_req = sdk_signed_random_requests(looper, sdk_wallet_client, 1)
    sdk_send_signed_requests(sdk_pool_handle, one_req)
    looper.runFor(waits.expectedPropagateTime(len(txnPoolNodeSet)) +
                  waits.expectedPrePrepareTime(len(txnPoolNodeSet)) +
                  waits.expectedPrepareTime(len(txnPoolNodeSet)) +
                  waits.expectedCommittedTime(len(txnPoolNodeSet)))

    delta.nodeIbStasher.reset_delays_and_process_delayeds('PROPAGATE')
    looper.runFor(waits.expectedPropagateTime(len(txnPoolNodeSet)) +
                  waits.expectedPrePrepareTime(len(txnPoolNodeSet)) +
                  waits.expectedPrepareTime(len(txnPoolNodeSet)) +
                  waits.expectedCommittedTime(len(txnPoolNodeSet)))

    for node in txnPoolNodeSet:
        node.nodeIbStasher.reset_delays_and_process_delayeds('COMMIT')
    looper.runFor(waits.expectedOrderingTime(delta.replicas.num_replicas))

    for node in txnPoolNodeSet:
        assert node.domainLedger.size - initial_ledger_size == 1


def test_preprepare_not_processed_if_any_request_is_already_in_3pc_process(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):

    nonPrNode = None
    for node in txnPoolNodeSet:
        if not node.has_master_primary:
            break

    nonPrNode = node

    initial_ledger_size = nonPrNode.domainLedger.size
    for node in txnPoolNodeSet:
        node.nodeIbStasher.delay(cDelay(300))

    one_req = sdk_signed_random_requests(looper, sdk_wallet_client, 1)
    sdk_send_signed_requests(sdk_pool_handle, one_req)
    looper.runFor(waits.expectedPropagateTime(len(txnPoolNodeSet)) +
                  waits.expectedPrePrepareTime(len(txnPoolNodeSet)))

    for node in txnPoolNodeSet:
        assert node.domainLedger.size == initial_ledger_size

    nonPrNode_m_replica = nonPrNode.master_replica
    params = nonPrNode_m_replica.spylog.getLastParams(nonPrNode_m_replica.processPrePrepare)
    assert params is not None
    ppPrevious = params["pre_prepare"]
    sender = params['sender']

    # to make really correct (acceptable by all other checks) PP we need update
    # ppTime, and root hashes for state and txn but we don't actually need that here
    ppWithDuplicateIdr = updateNamedTuple(ppPrevious, ppSeqNo=ppPrevious.ppSeqNo + 1)

    count = nonPrNode.spylog.count(nonPrNode.reportSuspiciousNode)
    nonPrNode_m_replica.processPrePrepare(ppWithDuplicateIdr, sender)
    assert nonPrNode.spylog.count(nonPrNode.reportSuspiciousNode) == count + 1

    params = nonPrNode.spylog.getLastParams(nonPrNode.reportSuspiciousNode)
    assert params is not None
    assert params['nodeName'] == nonPrNode_m_replica.getNodeName(sender)
    assert params['reason'] == Suspicions.PPR_INCLUDES_IN_3PC_PROCESS_REQUEST.reason
    assert params['code'] == Suspicions.PPR_INCLUDES_IN_3PC_PROCESS_REQUEST.code
    assert params['offendingMsg'].viewNo == ppWithDuplicateIdr.viewNo
    assert params['offendingMsg'].ppSeqNo == ppWithDuplicateIdr.ppSeqNo



def test_repeated_request_not_processed_after_view_change(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):

    delta = txnPoolNodeSet[3]
    initial_ledger_size = delta.domainLedger.size

    one_req = sdk_signed_random_requests(looper, sdk_wallet_client, 1)
    sdk_send_and_check(one_req, looper, txnPoolNodeSet, sdk_pool_handle)

    ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper, txnPoolNodeSet)

    sdk_send_signed_requests(sdk_pool_handle, one_req)
    looper.runFor(waits.expectedTransactionExecutionTime(len(txnPoolNodeSet)))

    for node in txnPoolNodeSet:
        assert node.domainLedger.size - initial_ledger_size == 1


def test_belated_request_not_processed_after_view_change(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):

    delta = txnPoolNodeSet[3]
    initial_ledger_size = delta.domainLedger.size
    delta.clientIbStasher.delay(req_delay(300))

    one_req = sdk_signed_random_requests(looper, sdk_wallet_client, 1)
    sdk_send_and_check(one_req, looper, txnPoolNodeSet, sdk_pool_handle)

    ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper, txnPoolNodeSet)

    delta.clientIbStasher.reset_delays_and_process_delayeds()
    looper.runFor(waits.expectedTransactionExecutionTime(len(txnPoolNodeSet)))

    for node in txnPoolNodeSet:
        assert node.domainLedger.size - initial_ledger_size == 1


def test_belated_propagate_not_processed_after_view_change(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):

    delta = txnPoolNodeSet[3]
    initial_ledger_size = delta.domainLedger.size
    delta.nodeIbStasher.delay(ppgDelay(300, 'Gamma'))

    one_req = sdk_signed_random_requests(looper, sdk_wallet_client, 1)
    sdk_send_and_check(one_req, looper, txnPoolNodeSet, sdk_pool_handle)

    ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper, txnPoolNodeSet)

    delta.nodeIbStasher.reset_delays_and_process_delayeds()
    looper.runFor(waits.expectedTransactionExecutionTime(len(txnPoolNodeSet)))

    for node in txnPoolNodeSet:
        assert node.domainLedger.size - initial_ledger_size == 1
