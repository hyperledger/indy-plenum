from time import perf_counter

import pytest

from plenum.common.config_helper import PNodeConfigHelper
from plenum.common.constants import DOMAIN_LEDGER_ID, LedgerState
from plenum.common.types import HA
from plenum.test import waits
from plenum.test.delayers import cr_delay
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    check_ledger_state
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.test_node import checkNodesConnected, TestNode
from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually

# Do not remove the next import

logger = getlogger()
txnCount = 5

@pytest.fixture(scope="module")
def tconf(tconf):
    oldMax3PCBatchSize = tconf.Max3PCBatchSize
    oldMax3PCBatchWait = tconf.Max3PCBatchWait
    tconf.Max3PCBatchSize = txnCount
    tconf.Max3PCBatchWait = 2
    yield tconf

    tconf.Max3PCBatchSize = oldMax3PCBatchSize
    tconf.Max3PCBatchWait = oldMax3PCBatchWait


# TODO: This test passes but it is observed that PREPAREs are not received at
# newly added node. If the stop and start steps are omitted then PREPAREs are
# received. Conclusion is that due to node restart, ZMQ is losing messages
# but its weird since prepares and commits are received which are sent before
# and after prepares, respectively. Here is the pivotal link
# https://www.pivotaltracker.com/story/show/127897273
def test_node_catchup_after_restart_with_txns(
        sdk_new_node_caught_up,
        txnPoolNodeSet,
        tdir,
        tconf,
        sdk_node_set_with_node_added_after_some_txns,
        allPluginsPath):
    """
    A node that restarts after some transactions should eventually get the
    transactions which happened while it was down
    :return:
    """
    looper, new_node, sdk_pool_handle, new_steward_wallet_handle = \
        sdk_node_set_with_node_added_after_some_txns
    logger.debug("Stopping node {} with pool ledger size {}".
                 format(new_node, new_node.poolManager.txnSeqNo))
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, new_node)
    looper.removeProdable(new_node)
    # for n in txnPoolNodeSet[:4]:
    #     for r in n.nodestack.remotes.values():
    #         if r.name == newNode.name:
    #             r.removeStaleCorrespondents()
    # looper.run(eventually(checkNodeDisconnectedFrom, newNode.name,
    #                       txnPoolNodeSet[:4], retryWait=1, timeout=5))
    # TODO: Check if the node has really stopped processing requests?
    logger.debug("Sending requests")
    more_requests = 5
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              new_steward_wallet_handle, more_requests)
    logger.debug("Starting the stopped node, {}".format(new_node))
    nodeHa, nodeCHa = HA(*new_node.nodestack.ha), HA(*new_node.clientstack.ha)
    config_helper = PNodeConfigHelper(new_node.name, tconf, chroot=tdir)
    newNode = TestNode(
        new_node.name,
        config_helper=config_helper,
        config=tconf,
        ha=nodeHa,
        cliha=nodeCHa,
        pluginPaths=allPluginsPath)
    looper.add(newNode)
    txnPoolNodeSet[-1] = newNode

    # Make sure ledger is not synced initially
    check_ledger_state(newNode, DOMAIN_LEDGER_ID, LedgerState.not_synced)

    # Delay catchup reply processing so LedgerState does not change
    # TODO fix delay, sometimes it's not enough and lower 'check_ledger_state'
    # fails because newNode's domain ledger state is 'synced'
    delay_catchup_reply = 10
    newNode.nodeIbStasher.delay(cr_delay(delay_catchup_reply))
    looper.run(checkNodesConnected(txnPoolNodeSet))

    # Make sure ledger starts syncing (sufficient consistency proofs received)
    looper.run(eventually(check_ledger_state, newNode, DOMAIN_LEDGER_ID,
                          LedgerState.syncing, retryWait=.5, timeout=50))

    confused_node = txnPoolNodeSet[0]
    new_node_leecher = newNode.ledgerManager._node_leecher._leechers[DOMAIN_LEDGER_ID]
    ct = new_node_leecher.catchup_till
    start, end = ct.start_size, ct.final_size
    cons_proof = confused_node.ledgerManager._node_seeder._build_consistency_proof(
        DOMAIN_LEDGER_ID, start, end)

    bad_send_time = None

    def chk():
        nonlocal bad_send_time
        entries = newNode.ledgerManager.spylog.getAll(newNode.ledgerManager.processConsistencyProof.__name__)
        for entry in entries:
            # `canProcessConsistencyProof` should return False after `syncing_time`
            if entry.starttime <= bad_send_time:
                continue
            cons_proof = entry.params['proof']
            service = newNode.ledgerManager._node_leecher._leechers[cons_proof.ledgerId]._cons_proof_service
            if not service._can_process_consistency_proof(cons_proof):
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
                         customTimeout=timeout,
                         exclude_from_check=['check_last_ordered_3pc_backup'])

    send_and_chk(LedgerState.synced)
