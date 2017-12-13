from plenum.common.request import Request
from plenum.common.util import get_utc_epoch
from stp_core.loop.eventually import eventually
from plenum.common.messages.node_messages import PrePrepare
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.helper import checkDiscardMsg
from plenum.test.view_change.helper import ensure_view_change
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import addNewStewardAndNode
from plenum.test.test_node import checkNodesConnected, \
    checkProtocolInstanceSetup, getPrimaryReplica
from plenum.test import waits


whitelist = ['found legacy entry']  # warnings


def testNodeDiscardMessageFromUnknownView(txnPoolNodeSet,
                                          nodeSetWithNodeAddedAfterSomeTxns,
                                          newNodeCaughtUp,
                                          allPluginsPath):
    """
    Node discards 3-phase or ViewChangeDone messages from view nos that it does not
    know of (view nos before it joined the pool)
    :return:
    """
    looper, nodeX, client, wallet, _, _ = nodeSetWithNodeAddedAfterSomeTxns
    viewNo = nodeX.viewNo

    # Force two view changes: node discards msgs which have viewNo
    # at least two less than node's. Current protocol implementation
    # needs to hold messages from the previous view as well as
    # from the current view.
    for i in range(2):
        ensure_view_change(looper, txnPoolNodeSet)
        waitNodeDataEquality(looper, nodeX, *txnPoolNodeSet[:-1])
        checkProtocolInstanceSetup(looper, txnPoolNodeSet, retryWait=1)

    sender = txnPoolNodeSet[0]
    rid_x_node = sender.nodestack.getRemote(nodeX.name).uid
    messageTimeout = waits.expectedNodeToNodeMessageDeliveryTime()

    # 3 pc msg (PrePrepare) needs to be discarded
    primaryRepl = getPrimaryReplica(txnPoolNodeSet)
    three_pc = PrePrepare(
        0,
        viewNo,
        10,
        get_utc_epoch(),
        [[wallet.defaultId, Request.gen_req_id()]],
        1,
        "random digest",
        DOMAIN_LEDGER_ID,
        primaryRepl.stateRootHash(DOMAIN_LEDGER_ID),
        primaryRepl.txnRootHash(DOMAIN_LEDGER_ID),
    )
    sender.send(three_pc, rid_x_node)
    looper.run(eventually(checkDiscardMsg, [nodeX, ], three_pc,
                          'un-acceptable viewNo',
                          retryWait=1, timeout=messageTimeout))

    # TODO: the same check for ViewChangeDone
