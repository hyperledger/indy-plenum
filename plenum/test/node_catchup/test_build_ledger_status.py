from plenum.common.constants import POOL_LEDGER_ID, DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID, CURRENT_PROTOCOL_VERSION
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.view_change.helper import ensure_view_change_complete


def check_ledger_status(ledger_status, ledger_id, node, is_last_ordered=True):
    ledger = node.getLedger(ledger_id)

    assert ledger_status
    assert ledger_status.ledgerId == ledger_id
    assert ledger_status.txnSeqNo == ledger.size
    assert ledger_status.merkleRoot == ledger.root_hash
    assert ledger_status.protocolVersion == CURRENT_PROTOCOL_VERSION

    if is_last_ordered:
        view_no, pp_seq_no = node.master_last_ordered_3PC
        assert ledger_status.viewNo is not None
        assert ledger_status.ppSeqNo is not None
        assert ledger_status.viewNo == view_no
        assert ledger_status.ppSeqNo == pp_seq_no
    else:
        assert ledger_status.viewNo is None
        assert ledger_status.ppSeqNo is None


def test_ledger_status_for_new_pool(txnPoolNodeSet):
    # we expect last ordered 3PC is None for all ledgers initially
    for node in txnPoolNodeSet:
        ls_pool = node.build_ledger_status(POOL_LEDGER_ID)
        check_ledger_status(ls_pool, POOL_LEDGER_ID, node, is_last_ordered=False)

        ls_domain = node.build_ledger_status(DOMAIN_LEDGER_ID)
        check_ledger_status(ls_domain, DOMAIN_LEDGER_ID, node, is_last_ordered=False)

        ls_config = node.build_ledger_status(CONFIG_LEDGER_ID)
        check_ledger_status(ls_config, CONFIG_LEDGER_ID, node, is_last_ordered=False)


def test_ledger_status_after_txn_ordered(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle):
    # we expect last ordered 3PC is not None for Domain ledger only, as there is a txn added to Domain ledger
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    for node in txnPoolNodeSet:
        ls_pool = node.build_ledger_status(POOL_LEDGER_ID)
        check_ledger_status(ls_pool, POOL_LEDGER_ID, node, is_last_ordered=False)

        ls_domain = node.build_ledger_status(DOMAIN_LEDGER_ID)
        check_ledger_status(ls_domain, DOMAIN_LEDGER_ID, node)

        ls_config = node.build_ledger_status(CONFIG_LEDGER_ID)
        check_ledger_status(ls_config, CONFIG_LEDGER_ID, node, is_last_ordered=False)


def test_ledger_status_after_catchup(looper, txnPoolNodeSet):
    # we expect last ordered 3PC is not None for all Nodes after catchup
    ensure_view_change_complete(looper, txnPoolNodeSet)
    for node in txnPoolNodeSet:
        for ledger_id in [POOL_LEDGER_ID, DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID]:
            ls = node.build_ledger_status(ledger_id)
            check_ledger_status(ls, ledger_id, node)


def test_ledger_status_for_new_node(looper, txnPoolNodeSet, sdk_node_created_after_some_txns):
    _, new_node, _, _ = sdk_node_created_after_some_txns
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet + [new_node])

    # check Ledger Status on a new Node (it should contain the last ordered 3PC for all ledgers)
    for ledger_id in [POOL_LEDGER_ID, DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID]:
        ls = new_node.build_ledger_status(ledger_id)
        check_ledger_status(ls, ledger_id, new_node)

    # Ledger Status for Pool ledger should return not None 3PC key as
    # a new Node txn was ordered
    for node in txnPoolNodeSet:
        ls_pool = node.build_ledger_status(POOL_LEDGER_ID)
        check_ledger_status(ls_pool, POOL_LEDGER_ID, node)
