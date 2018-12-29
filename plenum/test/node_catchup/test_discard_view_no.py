from plenum.common.request import Request
from plenum.common.util import get_utc_epoch
from plenum.server.replica_validator_enums import OLD_VIEW
from stp_core.loop.eventually import eventually
from plenum.common.messages.node_messages import PrePrepare
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.helper import checkDiscardMsg, init_discarded, create_pre_prepare_no_bls
from plenum.test.view_change.helper import ensure_view_change
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.test_node import checkProtocolInstanceSetup, \
    getPrimaryReplica
from plenum.test import waits

whitelist = ['found legacy entry']  # warnings


def testNodeDiscardMessageFromUnknownView(txnPoolNodeSet,
                                          sdk_node_set_with_node_added_after_some_txns,
                                          sdk_new_node_caught_up,
                                          allPluginsPath, sdk_wallet_client):
    """
    Node discards 3-phase or ViewChangeDone messages from view nos that it does not
    know of (view nos before it joined the pool)
    :return:
    """
    looper, new_node, sdk_pool_handle, new_steward_wallet_handle = \
        sdk_node_set_with_node_added_after_some_txns
    viewNo = new_node.viewNo

    # Force two view changes: node discards msgs which have viewNo
    # at least two less than node's. Current protocol implementation
    # needs to hold messages from the previous view as well as
    # from the current view.
    for i in range(2):
        ensure_view_change(looper, txnPoolNodeSet)
        waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1])
        checkProtocolInstanceSetup(looper, txnPoolNodeSet, retryWait=1)

    sender = txnPoolNodeSet[0]
    rid_x_node = sender.nodestack.getRemote(new_node.name).uid
    messageTimeout = waits.expectedNodeToNodeMessageDeliveryTime()

    # 3 pc msg (PrePrepare) needs to be discarded
    _, did = sdk_wallet_client
    primaryRepl = getPrimaryReplica(txnPoolNodeSet)
    inst_id = 0
    three_pc = create_pre_prepare_no_bls(primaryRepl.stateRootHash(DOMAIN_LEDGER_ID),
                                         viewNo,
                                         pp_seq_no=10,
                                         inst_id=inst_id)
    sender.send(three_pc, rid_x_node)
    looper.run(eventually(checkDiscardMsg, [new_node.replicas[inst_id], ], three_pc,
                          OLD_VIEW,
                          retryWait=1, timeout=messageTimeout))

    # TODO: the same check for ViewChangeDone
