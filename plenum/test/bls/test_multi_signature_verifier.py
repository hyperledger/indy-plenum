import pytest
import os

from plenum.bls.bls_crypto_factory import BlsFactoryIndyCrypto


@pytest.fixture()
def bls_crypto_factory(tdir_for_func):
    dir = os.path.join(tdir_for_func, 'Node1')
    os.mkdir(dir)
    return BlsFactoryIndyCrypto(dir)


def create_crypto_signer(bls_crypto_factory):
    bls_crypto_factory.generate_and_store_bls_keys()
    create_crypto_signer = bls_crypto_factory.create_bls_crypto_signer_from_saved_keys()
    return create_crypto_signer, create_crypto_signer.pk


def test_bls_crypto_verifier(bls_crypto_factory):
    bls_crypto_factory = bls_crypto_factory  # type: BlsFactoryIndyCrypto
    bls_crypto_verifier = bls_crypto_factory.create_bls_crypto_verifier()
    assert bls_crypto_verifier

    message = "some message"
    sigs = []
    keys = []
    for i in range(3):
        bls_crypto_signer, public_key = create_crypto_signer(bls_crypto_factory)
        keys.append(public_key)
        sigs.append(bls_crypto_signer.sign(message))

    multi_signature = bls_crypto_verifier.create_multi_sig(sigs)
    assert bls_crypto_verifier.verify_multi_sig(multi_signature, message, keys)
