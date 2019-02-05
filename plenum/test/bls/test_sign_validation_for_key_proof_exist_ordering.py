import pytest

from plenum.common.exceptions import PoolLedgerTimeoutException
from plenum.test.bls.helper import update_bls_keys_no_proof, \
    update_validate_bls_signature_without_key_proof
from plenum.test.helper import sdk_send_random_and_check

nodes_wth_bls = 0


@pytest.fixture(scope="module", params=[True, False])
def validate_bls_signature_without_key_proof(request):
    return request.param


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
    # update BLS keys with not checking for BLS key proof presence since we v alidate BLS signatures for
    # Pool Ledger now
    with update_validate_bls_signature_without_key_proof(txnPoolNodeSet, True):
        for n in txnPoolNodeSet:
            monkeypatch.setattr(n.poolManager.reqHandler, 'doStaticValidation', lambda req: True)
        for node_index in range(0, len(txnPoolNodeSet)):
            update_bls_keys_no_proof(node_index, sdk_wallet_stewards, sdk_pool_handle, looper, txnPoolNodeSet)
        monkeypatch.undo()

    with update_validate_bls_signature_without_key_proof(txnPoolNodeSet, validate_bls_signature_without_key_proof):
        if validate_bls_signature_without_key_proof:
            sdk_send_random_and_check(looper, txnPoolNodeSet,
                                      sdk_pool_handle, sdk_wallet_stewards[3], 1)
        else:
            with pytest.raises(PoolLedgerTimeoutException):
                sdk_send_random_and_check(looper, txnPoolNodeSet,
                                          sdk_pool_handle, sdk_wallet_stewards[3], 1)
