from logging import getLogger

from stp_core.loop.eventually import eventually

from plenum.test.malicious_behaviors_node import dont_send_propagate_to, dont_send_prepare_and_commit_to
from plenum.test.testing_utils import FakeSomething

from plenum.test.primary_selection.test_primary_selector import FakeNode

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit, MessageRep
from plenum.server.replica import Replica
from plenum.test import waits

from plenum.test.checkpoints.conftest import tconf, chkFreqPatched, \
    reqs_for_checkpoint
from plenum.test.helper import send_reqs_batches_and_get_suff_replies, sdk_send_batches_of_random_and_check, \
    sdk_send_batches_of_random
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    checkNodeDataForInequality
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node
from plenum.test.test_node import checkNodesConnected

from plenum.test.checkpoints.conftest import chkFreqPatched, reqs_for_checkpoint

CHK_FREQ = 5
LOG_SIZE = 3 * CHK_FREQ

ledger_id = 1


def test_incomplete_short_checkpoint_included_in_lag_for_catchup(
        looper, chkFreqPatched, reqs_for_checkpoint, txnPoolNodeSet,
        sdk_pool_handle, sdk_wallet_steward, sdk_wallet_client,
        tdir, tconf, allPluginsPath):
    master_node = txnPoolNodeSet[0]
    behind_node = txnPoolNodeSet[-1]
    rest_pool = txnPoolNodeSet[:-1]

    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                         sdk_wallet_steward, CHK_FREQ, CHK_FREQ)

    dont_send_prepare_and_commit_to(rest_pool, behind_node)

    sdk_send_batches_of_random(looper, txnPoolNodeSet, sdk_pool_handle,
                               sdk_wallet_steward, CHK_FREQ * 6, CHK_FREQ * 2)

    waitNodeDataEquality(looper, behind_node, *txnPoolNodeSet[:-1])

    # We clear catchuped requests
    assert len(behind_node.requests) == 0


def test_free_finalized_sended_catchuped_requests(
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


def test_free_finalized_non_sended_catchuped_requests(
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


def test_free_non_finalized_catchuped_requests(
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


def test_dont_free_finalized_sended_requests_if_more_caught_up_3pc(
        tdir, tconf):
    node = FakeNode(tdir, config=tconf)

    # "Processing request"
    req = FakeSomething(key='request_1', forwarded=True)
    key = req.key
    node.requests.add(req)
    node.requests.mark_as_forwarded(req, 3)
    node.replicas[0].prePrepares[(0, 2)] = FakeSomething(ledgerId=0, reqIdr=[key])
    node.replicas[0].requestQueues[ledger_id].add(key)

    # Request not freed and not deleted from requestQueues
    assert not node.requests.get(key).executed
    assert key in node.replicas[0].requestQueues[ledger_id]


def test_free_finalized_non_sended_if_more_caught_up_3pc(
        tdir, tconf):
    node = FakeNode(tdir, config=tconf)

    # "Processing request"
    req = FakeSomething(key='request_1', forwarded=True)
    key = req.key
    node.requests.add(req)
    node.requests.mark_as_forwarded(req, 3)
    node.replicas[0].requestQueues[ledger_id].add(key)

    # Request not freed and not deleted from requestQueues
    assert not node.requests.get(key).executed
    assert key in node.replicas[0].requestQueues[ledger_id]


def test_free_non_finalized_which_isnt_ordered(
        tdir, tconf):
    node = FakeNode(tdir, config=tconf)

    # "Processing request"
    req = FakeSomething(key='request_1', forwarded=False)
    key = req.key
    node.requests.add(req)

    # Request not deleted from requestQueues
    assert key in node.requests
