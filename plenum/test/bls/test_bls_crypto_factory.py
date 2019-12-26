import os

import pytest
from indy_crypto.bls import VerKey, ProofOfPossession

from crypto.bls.bls_crypto import BlsCryptoSigner
from crypto.bls.bls_key_manager import LoadBLSKeyError
from crypto.bls.indy_crypto.bls_crypto_indy_crypto import IndyCryptoBlsUtils, BlsCryptoSignerIndyCrypto
from plenum.bls.bls_crypto_factory import BlsFactoryIndyCrypto
from state.pruning_state import PruningState
from storage.kv_in_memory import KeyValueStorageInMemory


@pytest.fixture()
def pool_state():
    return PruningState(KeyValueStorageInMemory())


@pytest.fixture()
def bls_crypto_factory(tdir_for_func):
    keys_dir = os.path.join(tdir_for_func, 'Node1')
    os.mkdir(keys_dir)
    return BlsFactoryIndyCrypto(keys_dir)


@pytest.fixture()
def bls_crypto_factory2(tdir_for_func):
    keys_dir = os.path.join(tdir_for_func, 'Node2')
    os.mkdir(keys_dir)
    return BlsFactoryIndyCrypto(keys_dir)


def test_create_and_store_bls_keys(bls_crypto_factory):
    pk, key_proof = bls_crypto_factory.generate_and_store_bls_keys()
    assert pk
    assert isinstance(pk, str)
    assert bls_crypto_factory.create_bls_crypto_verifier() \
        .verify_key_proof_of_possession(IndyCryptoBlsUtils.bls_from_str(key_proof, cls=ProofOfPossession),
                                        IndyCryptoBlsUtils.bls_from_str(pk, cls=VerKey))


def test_create_bls_keys(bls_crypto_factory):
    sk, pk, key_proof = bls_crypto_factory.generate_bls_keys()
    assert pk
    assert sk
    assert isinstance(sk, str)
    assert isinstance(pk, str)
    assert bls_crypto_factory.create_bls_crypto_verifier() \
        .verify_key_proof_of_possession(IndyCryptoBlsUtils.bls_from_str(key_proof, cls=ProofOfPossession),
                                        IndyCryptoBlsUtils.bls_from_str(pk, cls=VerKey))


def test_create_and_store_bls_keys_multiple(bls_crypto_factory):
    pk1, key_proof1 = bls_crypto_factory.generate_and_store_bls_keys()
    pk2, key_proof2 = bls_crypto_factory.generate_and_store_bls_keys()
    pk3, key_proof3 = bls_crypto_factory.generate_and_store_bls_keys()
    assert pk1 != pk2 != pk3
    assert key_proof1 != key_proof2 != key_proof3


def test_create_bls_keys_multiple(bls_crypto_factory):
    sk1, pk1, _ = bls_crypto_factory.generate_bls_keys()
    sk2, pk2, _ = bls_crypto_factory.generate_bls_keys()
    sk3, pk3, _ = bls_crypto_factory.generate_bls_keys()
    assert pk1 != pk2 != pk3
    assert sk1 != sk2 != sk3


def test_create_bls_crypto_no_keys(bls_crypto_factory):
    with pytest.raises(LoadBLSKeyError):
        bls_crypto_factory.create_bls_crypto_signer_from_saved_keys()


def test_create_bls_crypto(bls_crypto_factory):
    pk, _ = bls_crypto_factory.generate_and_store_bls_keys()
    bls_crypto_signer = bls_crypto_factory.create_bls_crypto_signer_from_saved_keys()  # type: BlsCryptoSignerIndyCrypto
    assert bls_crypto_signer
    assert isinstance(bls_crypto_signer, BlsCryptoSigner)
    assert bls_crypto_signer._sk
    assert bls_crypto_signer.pk
    assert pk == IndyCryptoBlsUtils.bls_to_str(bls_crypto_signer.pk)


def test_create_bls_crypto_multiple_times(bls_crypto_factory):
    pk1, _ = bls_crypto_factory.generate_and_store_bls_keys()
    bls_crypto_signer1 = bls_crypto_factory.create_bls_crypto_signer_from_saved_keys()  # type: BlsCryptoSignerIndyCrypto
    assert pk1 == IndyCryptoBlsUtils.bls_to_str(bls_crypto_signer1.pk)

    pk2, _ = bls_crypto_factory.generate_and_store_bls_keys()
    bls_crypto_signer2 = bls_crypto_factory.create_bls_crypto_signer_from_saved_keys()  # type: BlsCryptoSignerIndyCrypto
    assert pk2 == IndyCryptoBlsUtils.bls_to_str(bls_crypto_signer2.pk)

    pk3, _ = bls_crypto_factory.generate_and_store_bls_keys()
    bls_crypto_signer3 = bls_crypto_factory.create_bls_crypto_signer_from_saved_keys()  # type: BlsCryptoSignerIndyCrypto
    assert pk3 == IndyCryptoBlsUtils.bls_to_str(bls_crypto_signer3.pk)


def test_bls_crypto_works(bls_crypto_factory, bls_crypto_factory2):
    # create bls signer for Node1
    bls_crypto_factory.generate_and_store_bls_keys()
    bls_crypto_signer1 = bls_crypto_factory.create_bls_crypto_signer_from_saved_keys()  # type: BlsCryptoSignerIndyCrypto
    pk1 = bls_crypto_signer1.pk

    # create bls signer for Node2
    bls_crypto_factory2.generate_and_store_bls_keys()
    bls_crypto_signer2 = bls_crypto_factory2.create_bls_crypto_signer_from_saved_keys()  # type: BlsCryptoSignerIndyCrypto
    pk2 = bls_crypto_signer2.pk

    # create bls verifier
    bls_crypto_verifier = bls_crypto_factory.create_bls_crypto_verifier()

    # each node signs the message
    msg = 'Hello!'.encode()
    pks = [pk1, pk2]
    sigs = []
    sigs.append(bls_crypto_signer1.sign(msg))
    sigs.append(bls_crypto_signer2.sign(msg))

    # each node creates multi-sig
    multi_sig1 = bls_crypto_verifier.create_multi_sig(sigs)
    multi_sig2 = bls_crypto_verifier.create_multi_sig(sigs)

    # each node verifies multi-sigs
    assert bls_crypto_verifier.verify_multi_sig(multi_sig1, msg, pks)
    assert bls_crypto_verifier.verify_multi_sig(multi_sig2, msg, pks)
