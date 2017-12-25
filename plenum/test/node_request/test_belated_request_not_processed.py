from plenum.test import waits
from plenum.test.delayers import cDelay, req_delay, ppgDelay
from plenum.test.helper import waitForSufficientRepliesForRequests, \
    randomOperation
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change


def test_repeated_request_not_processed_if_already_ordered(
        looper, txnPoolNodeSet, wallet1, client1, client1Connected):

    delta = txnPoolNodeSet[3]
    initial_ledger_size = delta.domainLedger.size

    req = wallet1.signOp(randomOperation())
    client1.submitReqs(req)
    waitForSufficientRepliesForRequests(looper, client1, requests=[req])

    client1.submitReqs(req)
    looper.runFor(waits.expectedTransactionExecutionTime(len(txnPoolNodeSet)))

    for node in txnPoolNodeSet:
        assert node.domainLedger.size - initial_ledger_size == 1


def test_belated_request_not_processed_if_already_ordered(
        looper, txnPoolNodeSet, wallet1, client1, client1Connected):

    delta = txnPoolNodeSet[3]
    initial_ledger_size = delta.domainLedger.size
    delta.clientIbStasher.delay(req_delay(300))

    req = wallet1.signOp(randomOperation())
    client1.submitReqs(req)
    waitForSufficientRepliesForRequests(looper, client1, requests=[req])

    delta.clientIbStasher.reset_delays_and_process_delayeds()
    looper.runFor(waits.expectedTransactionExecutionTime(len(txnPoolNodeSet)))

    for node in txnPoolNodeSet:
        assert node.domainLedger.size - initial_ledger_size == 1


def test_belated_propagate_not_processed_if_already_ordered(
        looper, txnPoolNodeSet, wallet1, client1, client1Connected):

    delta = txnPoolNodeSet[3]
    initial_ledger_size = delta.domainLedger.size
    delta.nodeIbStasher.delay(ppgDelay(300, 'Gamma'))

    req = wallet1.signOp(randomOperation())
    client1.submitReqs(req)
    waitForSufficientRepliesForRequests(looper, client1, requests=[req])

    delta.nodeIbStasher.reset_delays_and_process_delayeds()
    looper.runFor(waits.expectedTransactionExecutionTime(len(txnPoolNodeSet)))

    for node in txnPoolNodeSet:
        assert node.domainLedger.size - initial_ledger_size == 1


def test_repeated_request_not_processed_if_already_in_3pc_process(
        looper, txnPoolNodeSet, wallet1, client1, client1Connected):

    delta = txnPoolNodeSet[3]
    initial_ledger_size = delta.domainLedger.size
    for node in txnPoolNodeSet:
        node.nodeIbStasher.delay(cDelay(300))

    req = wallet1.signOp(randomOperation())
    client1.submitReqs(req)
    looper.runFor(waits.expectedPropagateTime(len(txnPoolNodeSet)) +
                  waits.expectedPrePrepareTime(len(txnPoolNodeSet)) +
                  waits.expectedPrepareTime(len(txnPoolNodeSet)) +
                  waits.expectedCommittedTime(len(txnPoolNodeSet)))

    client1.submitReqs(req)
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
        looper, txnPoolNodeSet, wallet1, client1, client1Connected):

    delta = txnPoolNodeSet[3]
    initial_ledger_size = delta.domainLedger.size
    delta.clientIbStasher.delay(req_delay(300))
    for node in txnPoolNodeSet:
        node.nodeIbStasher.delay(cDelay(300))

    req = wallet1.signOp(randomOperation())
    client1.submitReqs(req)
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
        looper, txnPoolNodeSet, wallet1, client1, client1Connected):

    delta = txnPoolNodeSet[3]
    initial_ledger_size = delta.domainLedger.size
    delta.nodeIbStasher.delay(ppgDelay(300, 'Gamma'))
    for node in txnPoolNodeSet:
        node.nodeIbStasher.delay(cDelay(300))

    req = wallet1.signOp(randomOperation())
    client1.submitReqs(req)
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


def test_repeated_request_not_processed_after_view_change(
        looper, txnPoolNodeSet, wallet1, client1, client1Connected):

    delta = txnPoolNodeSet[3]
    initial_ledger_size = delta.domainLedger.size

    req = wallet1.signOp(randomOperation())
    client1.submitReqs(req)
    waitForSufficientRepliesForRequests(looper, client1, requests=[req])

    ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper, txnPoolNodeSet)

    client1.submitReqs(req)
    looper.runFor(waits.expectedTransactionExecutionTime(len(txnPoolNodeSet)))

    for node in txnPoolNodeSet:
        assert node.domainLedger.size - initial_ledger_size == 1


def test_belated_request_not_processed_after_view_change(
        looper, txnPoolNodeSet, wallet1, client1, client1Connected):

    delta = txnPoolNodeSet[3]
    initial_ledger_size = delta.domainLedger.size
    delta.clientIbStasher.delay(req_delay(300))

    req = wallet1.signOp(randomOperation())
    client1.submitReqs(req)
    waitForSufficientRepliesForRequests(looper, client1, requests=[req])

    ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper, txnPoolNodeSet)

    delta.clientIbStasher.reset_delays_and_process_delayeds()
    looper.runFor(waits.expectedTransactionExecutionTime(len(txnPoolNodeSet)))

    for node in txnPoolNodeSet:
        assert node.domainLedger.size - initial_ledger_size == 1


def test_belated_propagate_not_processed_after_view_change(
        looper, txnPoolNodeSet, wallet1, client1, client1Connected):

    delta = txnPoolNodeSet[3]
    initial_ledger_size = delta.domainLedger.size
    delta.nodeIbStasher.delay(ppgDelay(300, 'Gamma'))

    req = wallet1.signOp(randomOperation())
    client1.submitReqs(req)
    waitForSufficientRepliesForRequests(looper, client1, requests=[req])

    ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper, txnPoolNodeSet)

    delta.nodeIbStasher.reset_delays_and_process_delayeds()
    looper.runFor(waits.expectedTransactionExecutionTime(len(txnPoolNodeSet)))

    for node in txnPoolNodeSet:
        assert node.domainLedger.size - initial_ledger_size == 1
