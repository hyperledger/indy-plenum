from plenum.common.constants import LEDGER_STATUS
from plenum.common.messages.fields import LedgerIdField, NonNegativeNumberField, \
    MerkleRootField
from plenum.common.messages.message_base import MessageBase
from plenum.test.helper import sdk_send_random_and_check, countDiscarded
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.node_catchup.test_config_ledger import start_stopped_node
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.test_node import checkNodesConnected
from plenum.common.types import f


def test_catchup_with_ledger_statuses_in_old_format_from_one_node(
        txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_steward,
        tconf, tdir, allPluginsPath):
    """
    A node is restarted and during a catch-up receives ledger statuses
    in an old format (without `protocolVersion`) from one of nodes in the pool.
    The test verifies that the node successfully completes the catch-up and
    participates in ordering of further transactions.
    """
    node_to_restart = txnPoolNodeSet[-1]
    other_nodes = txnPoolNodeSet[:-1]

    old_node = txnPoolNodeSet[0]

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 5)

    original_get_ledger_status = old_node.getLedgerStatus

    # Patch the method getLedgerStatus to
    # get_ledger_status_without_protocol_version for sending ledger status
    # in old format (without `protocolVersion`)

    def get_ledger_status_without_protocol_version(ledgerId: int):
        original_ledger_status = original_get_ledger_status(ledgerId)
        return LedgerStatusInOldFormat(original_ledger_status.ledgerId,
                                       original_ledger_status.txnSeqNo,
                                       original_ledger_status.viewNo,
                                       original_ledger_status.ppSeqNo,
                                       original_ledger_status.merkleRoot)

    old_node.getLedgerStatus = get_ledger_status_without_protocol_version

    # restart node
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            node_to_restart)
    looper.removeProdable(name=node_to_restart.name)
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward,
                              2)

    # add `node_to_restart` to pool
    node_to_restart = start_stopped_node(node_to_restart, looper, tconf,
                                         tdir, allPluginsPath)
    txnPoolNodeSet[-1] = node_to_restart
    looper.run(checkNodesConnected(txnPoolNodeSet))

    # Verify that `node_to_restart` successfully completes catch-up
    waitNodeDataEquality(looper, node_to_restart, *other_nodes)

    # check discarding ledger statuses from `old_node` for all ledgers
    assert countDiscarded(node_to_restart,
                          'replied message has invalid structure') >= 3

    # Verify that `node_to_restart` participates in ordering
    # of further transactions
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 5)
    waitNodeDataEquality(looper, node_to_restart, *other_nodes)


class LedgerStatusInOldFormat(MessageBase):
    """
    LedgerStatus class without protocol version
    """
    typename = LEDGER_STATUS
    schema = (
        (f.LEDGER_ID.nm, LedgerIdField()),
        (f.TXN_SEQ_NO.nm, NonNegativeNumberField()),
        (f.VIEW_NO.nm, NonNegativeNumberField(nullable=True)),
        (f.PP_SEQ_NO.nm, NonNegativeNumberField(nullable=True)),
        (f.MERKLE_ROOT.nm, MerkleRootField())
    )
