import pytest

from plenum.test.bls.helper import check_bls_key, update_bls_keys_no_proof, \
    update_validate_bls_signature_without_key_proof

nodes_wth_bls = 0


@pytest.fixture(scope="module", params=[True, False])
def validate_bls_signature_without_key_proof(request):
    return request.param


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
    # update BLS keys with not checking for BLS key proof presence since we v alidate BLS signatures for
    # Pool Ledger now
    with update_validate_bls_signature_without_key_proof(txnPoolNodeSet, True):
        for n in txnPoolNodeSet:
            monkeypatch.setattr(n.poolManager.reqHandler, 'doStaticValidation', lambda req: True)
        new_blspk = update_bls_keys_no_proof(0, sdk_wallet_stewards, sdk_pool_handle, looper, txnPoolNodeSet)
        monkeypatch.undo()

    with update_validate_bls_signature_without_key_proof(txnPoolNodeSet, validate_bls_signature_without_key_proof):
        if validate_bls_signature_without_key_proof:
            check_bls_key(new_blspk, txnPoolNodeSet[0], txnPoolNodeSet)
        else:
            for n in txnPoolNodeSet:
                assert n.bls_bft.bls_key_register.get_key_by_name(txnPoolNodeSet[0].name) is None
