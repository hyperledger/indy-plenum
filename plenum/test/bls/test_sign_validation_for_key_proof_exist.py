import pytest

from plenum.common.exceptions import PoolLedgerTimeoutException
from plenum.common.keygen_utils import init_bls_keys
from plenum.common.util import hexToFriendly
from plenum.test.bls.helper import check_bls_key
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import sdk_send_update_node, sdk_pool_refresh

nodes_wth_bls = 0


@pytest.fixture(scope="module", params=[True, False])
def validate_bls_signature_without_key_proof(txnPoolNodeSet, request):
    default_param = {}
    for n in txnPoolNodeSet:
        config = n.bls_bft.bls_key_register._pool_manager.config
        default_param[n.name] = config.VALIDATE_BLS_SIGNATURE_WITHOUT_KEY_PROOF
        config.VALIDATE_BLS_SIGNATURE_WITHOUT_KEY_PROOF = request.param
    yield request.param
    for n in txnPoolNodeSet:
        n.bls_bft.bls_key_register._pool_manager.\
            config.VALIDATE_BLS_SIGNATURE_WITHOUT_KEY_PROOF = default_param[n.name]


def test_switched_off_sign_validation_for_key_proof_exist(looper,
                                                          txnPoolNodeSet,
                                                          sdk_pool_handle,
                                                          sdk_wallet_stewards,
                                                          sdk_wallet_client,
                                                          monkeypatch,
                                                          validate_bls_signature_without_key_proof):
    '''
    Test that when VALIDATE_BLS_SIGNATURE_WITHOUT_KEY_PROOF = True, node use key sent without proof.
    Test that when VALIDATE_BLS_SIGNATURE_WITHOUT_KEY_PROOF = False, node does not use key sent without proof.
    '''
    for n in txnPoolNodeSet:
        monkeypatch.setattr(n.poolManager.reqHandler, 'doStaticValidation', lambda req: True)
    new_blspk = update_bls_keys_no_proof(0, sdk_wallet_stewards, sdk_pool_handle, looper, txnPoolNodeSet)
    monkeypatch.undo()
    if validate_bls_signature_without_key_proof:
        check_bls_key(new_blspk, txnPoolNodeSet[0], txnPoolNodeSet)
    else:
        for n in txnPoolNodeSet:
            assert n.bls_bft.bls_key_register.get_key_by_name(txnPoolNodeSet[0].name) is None


def test_ordering_with_nodes_have_not_bls_key_proofs(looper,
                                                     txnPoolNodeSet,
                                                     sdk_pool_handle,
                                                     sdk_wallet_stewards,
                                                     sdk_wallet_client,
                                                     monkeypatch,
                                                     validate_bls_signature_without_key_proof):
    '''
    Add BLS key without BLS key proof for all nodes. Test that when VALIDATE_BLS_SIGNATURE_WITHOUT_KEY_PROOF = False
    node does not use key sent without proof and transaction can not be ordered.
    And with VALIDATE_BLS_SIGNATURE_WITHOUT_KEY_PROOF = True transaction successfully ordered.
    '''

    for n in txnPoolNodeSet:
        monkeypatch.setattr(n.poolManager.reqHandler, 'doStaticValidation', lambda req: True)
    for node_index in range(0, len(txnPoolNodeSet)):
        update_bls_keys_no_proof(node_index, sdk_wallet_stewards, sdk_pool_handle, looper, txnPoolNodeSet)
    monkeypatch.undo()
    if validate_bls_signature_without_key_proof:
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_stewards[3], 1)
    else:
        with pytest.raises(PoolLedgerTimeoutException):
            sdk_send_random_and_check(looper, txnPoolNodeSet,
                                      sdk_pool_handle, sdk_wallet_stewards[3], 1)


def update_bls_keys_no_proof(node_index, sdk_wallet_stewards, sdk_pool_handle, looper, txnPoolNodeSet):
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
    return new_blspk
