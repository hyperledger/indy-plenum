from plenum.test.delayers import delay

from plenum.test.malicious_behaviors_node import dont_send_prepare_and_commit_to, dont_send_msgs_to
from plenum.test.testing_utils import FakeSomething

from plenum.test.primary_selection.test_primary_selector import FakeNode

from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit, Propagate
from plenum.test.helper import sdk_send_batches_of_random_and_check, \
    sdk_send_batches_of_random
from plenum.test.node_catchup.helper import waitNodeDataEquality

from plenum.test.checkpoints.conftest import chkFreqPatched, reqs_for_checkpoint

CHK_FREQ = 5
LOG_SIZE = 3 * CHK_FREQ

howlong = 100
ledger_id = 1
another_key = 'request_2'


def test_freeing_forwarded_sent_request(
        looper, chkFreqPatched, reqs_for_checkpoint, txnPoolNodeSet,
        sdk_pool_handle, sdk_wallet_steward):
    behind_node = txnPoolNodeSet[-1]
    rest_pool = txnPoolNodeSet[:-1]

    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                         sdk_wallet_steward, CHK_FREQ, CHK_FREQ)

    dont_send_prepare_and_commit_to(rest_pool, behind_node.name)

    sdk_send_batches_of_random(looper, txnPoolNodeSet, sdk_pool_handle,
                               sdk_wallet_steward, CHK_FREQ * 6, CHK_FREQ * 2)

    waitNodeDataEquality(looper, behind_node, *txnPoolNodeSet[:-1])

    # We clear catchuped requests
    assert len(behind_node.requests) == 0


def test_freeing_forwarded_not_sent_request(
        looper, chkFreqPatched, reqs_for_checkpoint, txnPoolNodeSet,
        sdk_pool_handle, sdk_wallet_steward):
    behind_node = txnPoolNodeSet[-1]
    rest_pool = txnPoolNodeSet[:-1]

    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                         sdk_wallet_steward, CHK_FREQ, CHK_FREQ)

    delay(PrePrepare, frm=rest_pool, to=behind_node, howlong=howlong)
    delay(Prepare, frm=rest_pool, to=behind_node, howlong=howlong)
    delay(Commit, frm=rest_pool, to=behind_node, howlong=howlong)

    sdk_send_batches_of_random(looper, txnPoolNodeSet, sdk_pool_handle,
                               sdk_wallet_steward, CHK_FREQ * 4, CHK_FREQ * 4)

    waitNodeDataEquality(looper, behind_node, *txnPoolNodeSet[:-1])

    # We clear catchuped requests
    assert len(behind_node.requests) == 0


def test_deletion_non_forwarded_not_sent_request(
        looper, chkFreqPatched, reqs_for_checkpoint, txnPoolNodeSet,
        sdk_pool_handle, sdk_wallet_steward):
    master_node = txnPoolNodeSet[0]
    behind_node = txnPoolNodeSet[-1]
    rest_pool = txnPoolNodeSet[:-1]

    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                         sdk_wallet_steward, CHK_FREQ, CHK_FREQ)

    dont_send_prepare_and_commit_to(rest_pool, behind_node.name)

    sdk_send_batches_of_random(looper, txnPoolNodeSet, sdk_pool_handle,
                               sdk_wallet_steward, CHK_FREQ * 6, CHK_FREQ * 2)

    waitNodeDataEquality(looper, behind_node, *txnPoolNodeSet[:-1])

    # We clear catchuped requests
    assert len(behind_node.requests) == 0


def test_free_forwarded_sended_catchuped_requests(
        tdir, tconf):
    node = FakeNode(tdir, config=tconf)

    # "Processing request"
    req = FakeSomething(key='request_1', forwarded=True)
    key = req.key
    node.requests.add(req)
    node.requests.mark_as_forwarded(req, 3)
    node.replicas[0].prePrepares[(0, 1)] = FakeSomething(ledgerId=0, reqIdr=[key])
    node.replicas[0].requestQueues[ledger_id].add(key)

    # "Catching up" request
    node.seqNoDB[key] = (True, True)
    node.ledgerManager.last_caught_up_3PC = (0, 1)
    node.allLedgersCaughtUp()

    # Request freed and deleted from requestQueues
    assert node.requests.get(key).executed
    assert not key in node.replicas[0].requestQueues[ledger_id]


def test_free_forwarded_non_sended_catchuped_requests(
        tdir, tconf):
    node = FakeNode(tdir, config=tconf)

    # "Processing request"
    req = FakeSomething(key='request_1', forwarded=True)
    key = req.key
    node.requests.add(req)
    node.requests.mark_as_forwarded(req, 3)
    node.replicas[0].requestQueues[ledger_id].add(key)

    # "Catching up" request
    node.seqNoDB[key] = (True, True)
    node.ledgerManager.last_caught_up_3PC = (0, 1)
    node.allLedgersCaughtUp()

    # Request freed and deleted from requestQueues
    assert node.requests.get(key).executed
    assert not key in node.replicas[0].requestQueues[ledger_id]


def test_free_non_forwarded_catchuped_requests(
        tdir, tconf):
    node = FakeNode(tdir, config=tconf)

    # "Processing request"
    req = FakeSomething(key='request_1', forwarded=False)
    key = req.key
    node.requests.add(req)

    # "Catching up" request
    node.seqNoDB[key] = (True, True)
    node.ledgerManager.last_caught_up_3PC = (0, 1)
    node.allLedgersCaughtUp()

    # Request deleted from requestQueues
    assert key not in node.requests


def test_dont_free_forwarded_sended_requests_if_more_caught_up_3pc(
        tdir, tconf):
    node = FakeNode(tdir, config=tconf)

    # "Processing request"
    req = FakeSomething(key='request_1', forwarded=True)
    key = req.key
    node.requests.add(req)
    node.requests.mark_as_forwarded(req, 3)
    node.replicas[0].prePrepares[(0, 2)] = FakeSomething(ledgerId=0, reqIdr=[key])
    node.replicas[0].requestQueues[ledger_id].add(key)

    # "Catching up" request
    node.seqNoDB[another_key] = (True, True)
    node.seqNoDB[key] = (False, False)
    node.ledgerManager.last_caught_up_3PC = (0, 1)
    node.allLedgersCaughtUp()

    # Request not freed and not deleted from requestQueues
    assert not node.requests.get(key).executed
    assert key in node.replicas[0].requestQueues[ledger_id]


def test_free_forwarded_non_sended_if_more_caught_up_3pc(
        tdir, tconf):
    node = FakeNode(tdir, config=tconf)

    # "Processing request"
    req = FakeSomething(key='request_1', forwarded=True)
    key = req.key
    node.requests.add(req)
    node.requests.mark_as_forwarded(req, 3)
    node.replicas[0].requestQueues[ledger_id].add(key)

    # "Catching up" request
    node.seqNoDB[another_key] = (True, True)
    node.seqNoDB[key] = (False, False)
    node.ledgerManager.last_caught_up_3PC = (0, 1)
    node.allLedgersCaughtUp()

    # Request not freed and not deleted from requestQueues
    assert not node.requests.get(key).executed
    assert key in node.replicas[0].requestQueues[ledger_id]


def test_free_non_forwarded_which_isnt_ordered(
        tdir, tconf):
    node = FakeNode(tdir, config=tconf)

    # "Processing request"
    req = FakeSomething(key='request_1', forwarded=False)
    key = req.key
    node.requests.add(req)

    # "Catching up" request
    node.seqNoDB[another_key] = (True, True)
    node.seqNoDB[key] = (False, False)
    node.ledgerManager.last_caught_up_3PC = (0, 1)
    node.allLedgersCaughtUp()

    # Request not deleted from requestQueues
    assert key in node.requests
