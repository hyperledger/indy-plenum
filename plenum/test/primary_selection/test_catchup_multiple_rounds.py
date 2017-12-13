import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.delayers import delay_3pc_messages, icDelay
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, \
    sendRandomRequests, waitForSufficientRepliesForRequests, checkViewNoForNodes
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper, nodeThetaAdded, \
    stewardAndWallet1, steward1, stewardWallet
from plenum.test.batching_3pc.conftest import tconf

from plenum.test.test_node import getNonPrimaryReplicas, getPrimaryReplica, \
    checkProtocolInstanceSetup
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually

Max3PCBatchSize = 3
TestRunningTimeLimitSec = 200


@pytest.mark.skip('Test incorrect')
def test_slow_nodes_catchup_before_selecting_primary_in_new_view(
        tconf,
        looper,
        txnPoolNodeSet,
        client1,
        wallet1,
        one_node_added,
        client1Connected):
    """
    Delay 3PC messages to one node and view change messages to some others
    (including primary) so the node that does not receive enough 3PC messages is
    behind but learns of the view change quickly and starts catchup.
    Other nodes learn of the view change late and thus keep on processing
    requests
    """
    new_node = one_node_added
    nprs = [r.node for r in getNonPrimaryReplicas(txnPoolNodeSet, 0)]
    primary_node = getPrimaryReplica(txnPoolNodeSet, 0).node
    slow_node = nprs[-1]
    # nodes_slow_to_inst_chg = [primary_node] + nprs[:2]
    nodes_slow_to_inst_chg = [n for n in txnPoolNodeSet if n != slow_node]
    delay_3pc = 100
    delay_ic = 5

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1,
                                        2 * Max3PCBatchSize)

    delay_3pc_messages([slow_node], 0, delay_3pc)

    for n in nodes_slow_to_inst_chg:
        n.nodeIbStasher.delay(icDelay(delay_ic))

    def start_count(): return sum([1 for e in slow_node.ledgerManager.spylog.getAll(
        slow_node.ledgerManager.startCatchUpProcess.__name__)
        if e.params['ledgerId'] == DOMAIN_LEDGER_ID])

    s = start_count()
    requests = sendRandomRequests(wallet1, client1, 10 * Max3PCBatchSize)

    ensure_view_change(looper, nodes=txnPoolNodeSet,
                       exclude_from_check=nodes_slow_to_inst_chg)

    waitForSufficientRepliesForRequests(looper, client1,
                                        requests=requests)
    waitNodeDataEquality(looper, slow_node, *txnPoolNodeSet[:-1])

    e = start_count()
    assert e - s >= 2

    looper.run(eventually(checkViewNoForNodes, slow_node.viewNo))
    checkProtocolInstanceSetup(looper, txnPoolNodeSet, retryWait=1)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1,
                                        2 * Max3PCBatchSize)

    waitNodeDataEquality(looper, new_node, *nodes_slow_to_inst_chg)
