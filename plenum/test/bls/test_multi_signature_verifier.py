import pytest
import os

from plenum.bls.bls_crypto_factory import BlsFactoryIndyCrypto


@pytest.fixture()
def bls_crypto_factory(tempdir):
    os.mkdir(os.path.join(tempdir, 'Node1'))
    return BlsFactoryIndyCrypto(tempdir, 'Node1')


def create_crypto(bls_crypto_factory):
    bls_crypto_factory.generate_and_store_bls_keys()
    bls_crypto = bls_crypto_factory.create_bls_crypto_from_saved_keys()
    return bls_crypto, bls_crypto.pk


def test_multi_signature_verifier(bls_crypto_factory):
    bls_crypto_factory =  bls_crypto_factory # type: BlsFactoryIndyCrypto
    verifier = bls_crypto_factory.create_multi_signature_verifier()
    assert verifier

    message = "some message"
    sigs = []
    keys = []
    for i in range(3):
        bls_crypto, public_key = create_crypto(bls_crypto_factory)
        keys.append(public_key)
        sigs.append(bls_crypto.sign(message))

    bls_crypto, _ = create_crypto(bls_crypto_factory)
    multi_signature = bls_crypto.create_multi_sig(sigs)
    assert bls_crypto.verify_multi_sig(multi_signature, message, keys)

