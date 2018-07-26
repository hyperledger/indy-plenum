import pytest

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


def test_node_catchup_with_new_ls_form(txnPoolNodeSet,
                                       looper,
                                       sdk_pool_handle,
                                       sdk_wallet_steward,
                                       tconf,
                                       tdir,
                                       allPluginsPath):
    '''
    One node restart and in catchup receive Ledger Status without protocol
    version from the one of nodes in pool.
    '''
    node_to_disconnect = txnPoolNodeSet[-1]
    break_node = txnPoolNodeSet[0]

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 5)

    original_get_ledger_status = break_node.getLedgerStatus

    # Path the method getLedgerStatus to
    # get_ledger_status_without_protocol_version for sending ledger status
    # without protocol version.

    def get_ledger_status_without_protocol_version(ledgerId: int):
        original_ledger_status = original_get_ledger_status(ledgerId)
        return CustomLedgerStatus(original_ledger_status.ledgerId,
                                  original_ledger_status.txnSeqNo,
                                  original_ledger_status.viewNo,
                                  original_ledger_status.ppSeqNo,
                                  original_ledger_status.merkleRoot)

    break_node.getLedgerStatus = get_ledger_status_without_protocol_version

    # restart node
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            node_to_disconnect)
    looper.removeProdable(name=node_to_disconnect.name)
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward,
                              2)

    # add node_to_disconnect to pool
    node_to_disconnect = start_stopped_node(node_to_disconnect, looper, tconf,
                                            tdir, allPluginsPath)
    txnPoolNodeSet[-1] = node_to_disconnect
    looper.run(checkNodesConnected(txnPoolNodeSet))
    waitNodeDataEquality(looper, node_to_disconnect, *txnPoolNodeSet)
    # check discarding a Ledger Statuses from the break_node for all ledgers
    assert countDiscarded(node_to_disconnect,
                          'replied message has invalid structure') >= 3
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 5)
    waitNodeDataEquality(looper, node_to_disconnect, *txnPoolNodeSet)


class CustomLedgerStatus(MessageBase):
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
