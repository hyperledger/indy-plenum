from plenum.common.constants import POOL_LEDGER_ID, DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID, CURRENT_PROTOCOL_VERSION
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.view_change.helper import ensure_view_change_complete


def check_ledger_status(ledger_status, ledger_id, node):
    ledger = node.getLedger(ledger_id)

    assert ledger_status
    assert ledger_status.ledgerId == ledger_id
    assert ledger_status.txnSeqNo == ledger.size
    assert ledger_status.merkleRoot == ledger.root_hash
    assert ledger_status.protocolVersion == CURRENT_PROTOCOL_VERSION


def check_ledger_statuses_on_node(node):
    check_ledger_status(node.build_ledger_status(POOL_LEDGER_ID),
                        POOL_LEDGER_ID, node)

    check_ledger_status(node.build_ledger_status(DOMAIN_LEDGER_ID),
                        DOMAIN_LEDGER_ID, node)

    check_ledger_status(node.build_ledger_status(CONFIG_LEDGER_ID),
                        CONFIG_LEDGER_ID, node)


def check_ledger_statuses(nodes):
    for node in nodes:
        check_ledger_statuses_on_node(node)


def test_ledger_status_for_new_pool(txnPoolNodeSet):
    # we expect last ordered 3PC is None for all ledgers initially
    check_ledger_statuses(txnPoolNodeSet)


def test_ledger_status_after_txn_ordered(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle):
    # we expect last ordered 3PC is not None for Domain ledger only, as there is a txn added to Domain ledger
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    check_ledger_statuses(txnPoolNodeSet)


def test_ledger_status_after_catchup(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle):
    # we expect last ordered 3PC is not None for Domain ledger only, as there is a txn added to Domain ledger
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)

    ensure_view_change_complete(looper, txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    check_ledger_statuses(txnPoolNodeSet)


def test_ledger_status_for_new_node(looper, txnPoolNodeSet, sdk_node_created_after_some_txns):
    _, new_node, sdk_pool_handle, new_steward_wallet_handle = sdk_node_created_after_some_txns
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, new_steward_wallet_handle, 1)

    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet + [new_node],
                                    exclude_from_check=['check_last_ordered_3pc_backup'])

    # Ledger Status for Pool ledger should return not None 3PC key as
    # a new Node txn was ordered
    check_ledger_statuses(txnPoolNodeSet)

    # check Ledger Status on a new Node (it should contain the same last ordered 3PC as on others)
    check_ledger_statuses_on_node(new_node)