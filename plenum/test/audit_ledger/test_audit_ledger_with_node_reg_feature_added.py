from plenum.common.constants import AUDIT_TXN_NODE_REG
from plenum.common.txn_util import get_payload_data
from plenum.test.helper import sdk_send_random_and_check


def patch_node_reg_in_audit(node, monkeypatch):
    monkeypatch.setattr(node.write_manager.audit_b_handler,
                        '_fill_node_reg',
                        lambda txn, three_pc_batch, last_audit_txn: None)
    monkeypatch.setattr(node.master_replica._ordering_service,
                        '_get_node_reg_for_ordered',
                        lambda pp: node.primaries)


def test_audit_ledger_with_node_reg_feature_added(looper, tconf, txnPoolNodeSet, monkeypatch,
                                                  sdk_pool_handle, sdk_wallet_client):
    # 1. patch audit ledger to not store node reg at all
    for n in txnPoolNodeSet:
        patch_node_reg_in_audit(n, monkeypatch)

    # 2. order a txn
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)

    # 3. check that node reg is not present in the latest audit txn
    for node in txnPoolNodeSet:
        assert AUDIT_TXN_NODE_REG not in get_payload_data(node.auditLedger.get_last_txn())

    # 4. unpatch so that node regs are written now
    monkeypatch.undo()

    # 5. order a txn
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)

    # 6. check that node reg is present in the latest audit txn
    for node in txnPoolNodeSet:
        assert get_payload_data(node.auditLedger.get_last_txn())[AUDIT_TXN_NODE_REG] == ['Alpha', 'Beta', 'Gamma',
                                                                                         'Delta']
