from logging import getLogger

import pytest
from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit, Checkpoint

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.delayers import cDelay, delay_3pc_messages

from plenum.test.helper import send_reqs_batches_and_get_suff_replies
from plenum.test.node_catchup.helper import waitNodeDataInequality, waitNodeDataEquality
from plenum.test.test_node import getNonPrimaryReplicas


logger = getLogger()

TestRunningTimeLimitSec = 200


def test_node_catchup_after_checkpoints(
        looper,
        chk_freq_patched,
        txnPoolNodeSet,
        wallet1,
        client1,
        client1Connected,
        broken_node_and_others):
    """
    For some reason a node misses 3pc messages but eventually the node stashes
    some amount checkpoints and decides to catchup.
    """
    broken_node, other_nodes = broken_node_and_others
    completed_catchups_before = get_number_of_completed_catchups(broken_node)

    logger.info("Step 1: The node misses quite a lot requests")
    send_reqs_batches_and_get_suff_replies(looper, wallet1, client1,
                                           num_reqs=chk_freq_patched + 1,
                                           num_batches=chk_freq_patched + 1,
                                           )
    waitNodeDataInequality(looper, broken_node, *other_nodes)

    logger.info(
        "Step 2: The node gets requests but cannot process them because of "
        "missed ones. But the nodes eventually stashes some amount checkpoints "
        "after that the node starts catch up")
    repaired_node = repair_broken_node(broken_node)
    send_reqs_batches_and_get_suff_replies(looper, wallet1, client1,
                                           num_reqs=2 * chk_freq_patched,
                                           num_batches=2 * chk_freq_patched
                                           )
    waitNodeDataEquality(looper, repaired_node, *other_nodes)

    # check if there was at least 1 catchup
    completed_catchups_after = get_number_of_completed_catchups(repaired_node)
    assert completed_catchups_after >= completed_catchups_before + 1

    logger.info("Step 3: Check if the node is able to process requests")
    send_reqs_batches_and_get_suff_replies(looper, wallet1, client1,
                                           num_reqs=chk_freq_patched + 2,
                                           num_batches=chk_freq_patched + 2
                                           )
    waitNodeDataEquality(looper, repaired_node, *other_nodes)


@pytest.fixture
def broken_node_and_others(txnPoolNodeSet):
    node = getNonPrimaryReplicas(txnPoolNodeSet, 0)[-1].node
    other = [n for n in txnPoolNodeSet if n != node]
    node._backup_send_to_replica = node.sendToReplica

    def brokenSendToReplica(msg, frm):
        logger.warning(
            "{} is broken. 'sendToReplica' does nothing".format(node.name))

    node.nodeMsgRouter.extend(
        (
            (PrePrepare, brokenSendToReplica),
            (Prepare, brokenSendToReplica),
            (Commit, brokenSendToReplica),
            (Checkpoint, brokenSendToReplica),
        )
    )

    return node, other


def repair_broken_node(node):
    node.nodeMsgRouter.extend(
        (
            (PrePrepare, node.sendToReplica),
            (Prepare, node.sendToReplica),
            (Commit, node.sendToReplica),
            (Checkpoint, node.sendToReplica),
        )
    )
    return node


@pytest.fixture(scope="module")
def chk_freq_patched(tconf, request):
    oldChkFreq = tconf.CHK_FREQ
    oldLogSize = tconf.LOG_SIZE

    tconf.CHK_FREQ = 5
    tconf.LOG_SIZE = 3 * tconf.CHK_FREQ

    def reset():
        tconf.CHK_FREQ = oldChkFreq
        tconf.LOG_SIZE = oldLogSize

    request.addfinalizer(reset)

    return tconf.CHK_FREQ


def get_number_of_completed_catchups(node):
    cnt = 0
    for entry in node.ledgerManager.spylog.getAll(
            node.ledgerManager.catchupCompleted):
        if entry.params['ledgerId'] == DOMAIN_LEDGER_ID:
            cnt += 1
    return cnt
