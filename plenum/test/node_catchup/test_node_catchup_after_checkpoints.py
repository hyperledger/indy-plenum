from logging import getLogger

import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.delayers import cDelay, delay_3pc_messages

from plenum.test.helper import send_reqs_batches_and_get_suff_replies
from plenum.test.node_catchup.helper import waitNodeDataInequality, waitNodeDataEquality
from plenum.test.test_node import getNonPrimaryReplicas


logger = getLogger()


def test_node_catchup_after_checkpoints(looper, txnPoolNodeSet, chk_freq_patched,
                                        wallet1, client1, client1Connected,
                                        slow_node_and_others):
    """
    For some reason a node misses 3pc messages but eventually the node stashes
    some amount checkpoints and decides to catchup.
    """
    slow_node, other_nodes = slow_node_and_others
    completed_catchups_before = get_number_of_completed_catchups(slow_node)

    logger.info("Step 1: Send less than required for start the catchup procedure"
                "and check the slow node falls behind")
    batches_num = 2 * chk_freq_patched - 1
    send_reqs_batches_and_get_suff_replies(looper, wallet1, client1,
                                           num_reqs=batches_num,
                                           num_batches=batches_num,
                                           )
    waitNodeDataInequality(looper, slow_node, *other_nodes)

    logger.info("Step 2: Send remaining requests in order to trigger the catchup"
                "procedure for the slow node, then check data equality")
    send_reqs_batches_and_get_suff_replies(looper, wallet1, client1,
                                           num_reqs=1
                                           )
    waitNodeDataEquality(looper, slow_node, *other_nodes)
    # check if there was a catchup
    completed_catchups_after = get_number_of_completed_catchups(slow_node)
    assert completed_catchups_after == completed_catchups_before + 1


@pytest.fixture
def slow_node_and_others(txnPoolNodeSet):
    node = getNonPrimaryReplicas(txnPoolNodeSet, 0)[-1].node
    other = [n for n in txnPoolNodeSet if n != node]

    delay = 1000
    logger.info("Delay 3pc messages for {} on {} sec".format(node, delay))

    node.nodeIbStasher.delay(
        cDelay(delay_3pc_messages([node, ], inst_id=None, delay=delay))
    )
    return node, other


@pytest.fixture(scope="module")
def chk_freq_patched(tconf, request):
    oldChkFreq = tconf.CHK_FREQ
    oldLogSize = tconf.LOG_SIZE

    tconf.CHK_FREQ = 2
    tconf.LOG_SIZE = 2*tconf.CHK_FREQ

    def reset():
        tconf.CHK_FREQ = oldChkFreq
        tconf.LOG_SIZE = oldLogSize

    request.addfinalizer(reset)

    return tconf.CHK_FREQ


def get_number_of_completed_catchups(node):
    cnt = 0
    for entry in node.ledgerManager.spylog.getAll(node.ledgerManager.catchupCompleted):
        if entry.params['ledgerId'] == DOMAIN_LEDGER_ID:
            cnt += 1
    return cnt

