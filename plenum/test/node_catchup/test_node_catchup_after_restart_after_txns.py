from time import perf_counter

import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID, LedgerState
from plenum.test.delayers import cr_delay
from plenum.test.spy_helpers import get_count

from stp_core.loop.eventually import eventually
from plenum.common.types import HA
from stp_core.common.log import getlogger
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, \
    check_last_ordered_3pc
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    check_ledger_state
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.test_ledger_manager import TestLedgerManager
from plenum.test.test_node import checkNodesConnected, TestNode
from plenum.test import waits
from plenum.common.config_helper import PNodeConfigHelper

# Do not remove the next import
from plenum.test.node_catchup.conftest import whitelist

logger = getlogger()
txnCount = 5


# TODO: This test passes but it is observed that PREPAREs are not received at
# newly added node. If the stop and start steps are omitted then PREPAREs are
# received. Conclusion is that due to node restart, ZMQ is losing messages
# but its weird since prepares and commits are received which are sent before
# and after prepares, respectively. Here is the pivotal link
# https://www.pivotaltracker.com/story/show/127897273
def test_node_catchup_after_restart_with_txns(
        newNodeCaughtUp,
        txnPoolNodeSet,
        tdir,
        tconf,
        nodeSetWithNodeAddedAfterSomeTxns,
        allPluginsPath):
    """
    A node that restarts after some transactions should eventually get the
    transactions which happened while it was down
    :return:
    """
    looper, newNode, client, wallet, _, _ = nodeSetWithNodeAddedAfterSomeTxns
    logger.debug("Stopping node {} with pool ledger size {}".
                 format(newNode, newNode.poolManager.txnSeqNo))
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, newNode)
    looper.removeProdable(newNode)
    # for n in txnPoolNodeSet[:4]:
    #     for r in n.nodestack.remotes.values():
    #         if r.name == newNode.name:
    #             r.removeStaleCorrespondents()
    # looper.run(eventually(checkNodeDisconnectedFrom, newNode.name,
    #                       txnPoolNodeSet[:4], retryWait=1, timeout=5))
    # TODO: Check if the node has really stopped processing requests?
    logger.debug("Sending requests")
    more_requests = 5
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, more_requests)
    logger.debug("Starting the stopped node, {}".format(newNode))
    nodeHa, nodeCHa = HA(*newNode.nodestack.ha), HA(*newNode.clientstack.ha)
    config_helper = PNodeConfigHelper(newNode.name, tconf, chroot=tdir)
    newNode = TestNode(
        newNode.name,
        config_helper=config_helper,
        config=tconf,
        ha=nodeHa,
        cliha=nodeCHa,
        pluginPaths=allPluginsPath)
    looper.add(newNode)
    txnPoolNodeSet[-1] = newNode

    # Delay catchup reply processing so LedgerState does not change
    # TODO fix delay, sometimes it's not enough and loweer 'check_ledger_state'
    # fails because newNode's domain ledger state is 'synced'
    delay_catchup_reply = 5
    newNode.nodeIbStasher.delay(cr_delay(delay_catchup_reply))
    looper.run(checkNodesConnected(txnPoolNodeSet))

    # Make sure ledger starts syncing (sufficient consistency proofs received)
    looper.run(eventually(check_ledger_state, newNode, DOMAIN_LEDGER_ID,
                          LedgerState.syncing, retryWait=.5, timeout=5))

    confused_node = txnPoolNodeSet[0]
    new_node_ledger = newNode.ledgerManager.ledgerRegistry[DOMAIN_LEDGER_ID]
    cp = new_node_ledger.catchUpTill
    start, end = cp.seqNoStart, cp.seqNoEnd
    cons_proof = confused_node.ledgerManager._buildConsistencyProof(
        DOMAIN_LEDGER_ID, start, end)

    bad_send_time = None

    def chk():
        nonlocal bad_send_time
        entries = newNode.ledgerManager.spylog.getAll(
            newNode.ledgerManager.canProcessConsistencyProof.__name__)
        for entry in entries:
            # `canProcessConsistencyProof` should return False after `syncing_time`
            if entry.result == False and entry.starttime > bad_send_time:
                return
        assert False

    def send_and_chk(ledger_state):
        nonlocal bad_send_time, cons_proof
        bad_send_time = perf_counter()
        confused_node.ledgerManager.sendTo(cons_proof, newNode.name)
        # Check that the ConsistencyProof messages rejected
        looper.run(eventually(chk, retryWait=.5, timeout=5))
        check_ledger_state(newNode, DOMAIN_LEDGER_ID, ledger_state)

    send_and_chk(LedgerState.syncing)

    # Not accurate timeout but a conservative one
    timeout = waits.expectedPoolGetReadyTimeout(len(txnPoolNodeSet)) + \
        2 * delay_catchup_reply
    waitNodeDataEquality(looper, newNode, *txnPoolNodeSet[:-1],
                         customTimeout=timeout)
    assert new_node_ledger.num_txns_caught_up == more_requests
    send_and_chk(LedgerState.synced)
