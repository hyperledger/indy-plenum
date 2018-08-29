import pytest

from plenum.common.exceptions import PoolLedgerTimeoutException
from plenum.common.keygen_utils import init_bls_keys
from plenum.common.util import hexToFriendly
from plenum.test.bls.helper import check_bls_key
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import sdk_send_update_node, sdk_pool_refresh

nodes_wth_bls = 0


def test_switched_off_sign_validation_for_key_proof_exist(looper,
                                                          txnPoolNodeSet,
                                                          sdk_pool_handle,
                                                          sdk_wallet_stewards,
                                                          sdk_wallet_client,
                                                          monkeypatch):
    '''
    Add BLS key without BLS key proof. Test that when ALIDATE_SIGN_WITHOUT_BLS_KEY_PROOF = True,
    node use key sent without proof.
    '''
    for n in txnPoolNodeSet:
        monkeypatch.setattr(n.poolManager.reqHandler, 'doStaticValidation', lambda req: True)
        n.bls_bft.bls_key_register._pool_manager.config.VALIDATE_SIGN_WITHOUT_BLS_KEY_PROOF = True
    new_blspk = update_bls_keys(0, sdk_wallet_stewards, sdk_pool_handle, looper, txnPoolNodeSet)
    monkeypatch.undo()
    check_bls_key(new_blspk, txnPoolNodeSet[0], txnPoolNodeSet)


def test_switched_on_sign_validation_for_key_proof_exist(looper,
                                                         txnPoolNodeSet,
                                                         sdk_pool_handle,
                                                         sdk_wallet_stewards,
                                                         sdk_wallet_client,
                                                         monkeypatch):
    '''
    Add BLS key without BLS key proof. Test that when VALIDATE_SIGN_WITHOUT_BLS_KEY_PROOF = False
    node does not use key sent without proof.
    '''

    for n in txnPoolNodeSet:
        monkeypatch.setattr(n.poolManager.reqHandler, 'doStaticValidation', lambda req: True)
        n.bls_bft.bls_key_register._pool_manager.config.VALIDATE_SIGN_WITHOUT_BLS_KEY_PROOF = False
    update_bls_keys(0, sdk_wallet_stewards, sdk_pool_handle, looper, txnPoolNodeSet)
    monkeypatch.undo()
    for n in txnPoolNodeSet:
        assert n.bls_bft.bls_key_register.get_key_by_name(txnPoolNodeSet[0].name) is None


def test_fail_ordering_with_nodes_sign_without_key_proof(looper,
                                                          txnPoolNodeSet,
                                                          sdk_pool_handle,
                                                          sdk_wallet_stewards,
                                                          sdk_wallet_client,
                                                          monkeypatch):
    '''
    Add BLS key without BLS key proof for 3 nodes. Test that when VALIDATE_SIGN_WITHOUT_BLS_KEY_PROOF = False
    node does not use key sent without proof and transaction can not be ordered.
    '''

    for n in txnPoolNodeSet:
        monkeypatch.setattr(n.poolManager.reqHandler, 'doStaticValidation', lambda req: True)
        n.bls_bft.bls_key_register._pool_manager.config.VALIDATE_SIGN_WITHOUT_BLS_KEY_PROOF = True
    for node_index in range(0, len(txnPoolNodeSet) - 1):
        update_bls_keys(node_index, sdk_wallet_stewards, sdk_pool_handle, looper, txnPoolNodeSet)
    monkeypatch.undo()
    for n in txnPoolNodeSet:
        n.bls_bft.bls_key_register._pool_manager.config.VALIDATE_SIGN_WITHOUT_BLS_KEY_PROOF = False
    with pytest.raises(PoolLedgerTimeoutException):
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_stewards[3], 1, total_timeout=10)


def update_bls_keys(node_index, sdk_wallet_stewards, sdk_pool_handle, looper, txnPoolNodeSet):
    node = txnPoolNodeSet[node_index]
    sdk_wallet_steward = sdk_wallet_stewards[node_index]
    new_blspk, key_proof = init_bls_keys(node.keys_dir, node.name)
    node_dest = hexToFriendly(node.nodestack.verhex)
    sdk_send_update_node(looper, sdk_wallet_steward,
                         sdk_pool_handle,
                         node_dest, node.name,
                         None, None,
                         None, None,
                         bls_key=new_blspk,
                         services=None,
                         key_proof=None)
    poolSetExceptOne = list(txnPoolNodeSet)
    poolSetExceptOne.remove(node)
    waitNodeDataEquality(looper, node, *poolSetExceptOne)
    sdk_pool_refresh(looper, sdk_pool_handle)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_steward, sdk_pool_handle)
    return new_blspk
