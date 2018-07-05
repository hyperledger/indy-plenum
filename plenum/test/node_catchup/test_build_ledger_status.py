from plenum.common.constants import POOL_LEDGER_ID, DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID, CURRENT_PROTOCOL_VERSION
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.view_change.helper import ensure_view_change_complete


def check_ledger_status(ledger_status, ledger_id, node, last_ordered_3pc=(None, None)):
    ledger = node.getLedger(ledger_id)

    assert ledger_status
    assert ledger_status.ledgerId == ledger_id
    assert ledger_status.txnSeqNo == ledger.size
    assert ledger_status.merkleRoot == ledger.root_hash
    assert ledger_status.protocolVersion == CURRENT_PROTOCOL_VERSION

    (view_no, pp_seq_no) = last_ordered_3pc
    assert ledger_status.viewNo == view_no
    assert ledger_status.ppSeqNo == pp_seq_no


def check_ledger_statuses_on_node(node,
                                  pool_last_ordered_3pc=(None, None),
                                  domain_last_ordered_3pc=(None, None),
                                  config_last_ordered_3pc=(None, None)):
    check_ledger_status(node.build_ledger_status(POOL_LEDGER_ID),
                        POOL_LEDGER_ID, node,
                        last_ordered_3pc=pool_last_ordered_3pc)

    check_ledger_status(node.build_ledger_status(DOMAIN_LEDGER_ID),
                        DOMAIN_LEDGER_ID, node,
                        last_ordered_3pc=domain_last_ordered_3pc)

    check_ledger_status(node.build_ledger_status(CONFIG_LEDGER_ID),
                        CONFIG_LEDGER_ID, node,
                        last_ordered_3pc=config_last_ordered_3pc)


def check_ledger_statuses(nodes,
                          pool_last_ordered_3pc=(None, None),
                          domain_last_ordered_3pc=(None, None),
                          config_last_ordered_3pc=(None, None)):
    for node in nodes:
        check_ledger_statuses_on_node(node,
                                      pool_last_ordered_3pc,
                                      domain_last_ordered_3pc,
                                      config_last_ordered_3pc)


def test_ledger_status_for_new_pool(txnPoolNodeSet):
    # we expect last ordered 3PC is None for all ledgers initially
    check_ledger_statuses(txnPoolNodeSet,
                          pool_last_ordered_3pc=(None, None),
                          domain_last_ordered_3pc=(None, None),
                          config_last_ordered_3pc=(None, None))


def test_ledger_status_after_txn_ordered(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle):
    # we expect last ordered 3PC is not None for Domain ledger only, as there is a txn added to Domain ledger
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    check_ledger_statuses(txnPoolNodeSet,
                          pool_last_ordered_3pc=(None, None),
                          domain_last_ordered_3pc=txnPoolNodeSet[0].master_last_ordered_3PC,
                          config_last_ordered_3pc=(None, None))


def test_ledger_status_after_catchup(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle):
    # we expect last ordered 3PC is not None for Domain ledger only, as there is a txn added to Domain ledger
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)

    ensure_view_change_complete(looper, txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    check_ledger_statuses(txnPoolNodeSet,
                          pool_last_ordered_3pc=(None, None),
                          domain_last_ordered_3pc=txnPoolNodeSet[0].master_last_ordered_3PC,
                          config_last_ordered_3pc=(None, None))


def test_ledger_status_for_new_node(looper, txnPoolNodeSet, sdk_node_created_after_some_txns):
    _, new_node, _, _ = sdk_node_created_after_some_txns
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet + [new_node])

    ref_node = txnPoolNodeSet[0]
    pool_last_ordered_3pc = ref_node.master_last_ordered_3PC  # since last txn is NODE
    domain_last_ordered_3pc = (ref_node.master_last_ordered_3PC[0], ref_node.master_last_ordered_3PC[1] - 1)

    # Ledger Status for Pool ledger should return not None 3PC key as
    # a new Node txn was ordered
    check_ledger_statuses(txnPoolNodeSet,
                          pool_last_ordered_3pc=pool_last_ordered_3pc,
                          domain_last_ordered_3pc=domain_last_ordered_3pc,
                          config_last_ordered_3pc=(None, None))

    # check Ledger Status on a new Node (it should contain the same last ordered 3PC as on others)
    check_ledger_statuses_on_node(new_node,
                                  pool_last_ordered_3pc=pool_last_ordered_3pc,
                                  domain_last_ordered_3pc=domain_last_ordered_3pc,
                                  config_last_ordered_3pc=(None, None))
