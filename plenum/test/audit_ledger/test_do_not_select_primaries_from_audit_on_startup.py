from plenum.common.constants import AUDIT_TXN_PRIMARIES
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.restart.helper import restart_nodes
from plenum.test.test_node import ensureElectionsDone


def fake_fill_audit_primaries(txn, three_pc_batch, last_audit_txn):
    txn[AUDIT_TXN_PRIMARIES] = ['Delta']


def patch_primaries_in_audit(node, monkeypatch):
    monkeypatch.setattr(node.write_manager.audit_b_handler,
                        '_fill_primaries',
                        fake_fill_audit_primaries)


def test_first_audit_catchup_during_ordering(monkeypatch,
                                             looper, tconf, tdir, allPluginsPath, txnPoolNodeSet,
                                             sdk_pool_handle, sdk_wallet_client):
    # 1. patch primaries in audit ledger
    for n in txnPoolNodeSet:
        patch_primaries_in_audit(n, monkeypatch)

    # 2. order a txn
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)

    # 3. restart Nodes 3 and 4
    restart_nodes(looper, txnPoolNodeSet, txnPoolNodeSet[2:], tconf, tdir, allPluginsPath, start_one_by_one=False)
    for n in txnPoolNodeSet[2:]:
        patch_primaries_in_audit(n, monkeypatch)

    # 5. make sure that all node have equal Priamries and can order
    ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=30)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet, custom_timeout=20)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
