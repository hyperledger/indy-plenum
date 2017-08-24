import os

import pytest
from crypto.bls.bls_key_manager import LoadBLSKeyError
from plenum.server.bls.bls import BlsFactoryCharm


@pytest.fixture()
def tempdir(tmpdir_factory):
    return tmpdir_factory.mktemp('').strpath


@pytest.fixture()
def bls_factory(tempdir):
    os.mkdir(os.path.join(tempdir, 'Node1'))
    return BlsFactoryCharm(tempdir, 'Node1')


@pytest.fixture()
def bls_factory2(tempdir):
    os.mkdir(os.path.join(tempdir, 'Node2'))
    return BlsFactoryCharm(tempdir, 'Node2')


@pytest.fixture()
def serializer(bls_factory):
    return bls_factory.create_serializer()


def test_create_bls_keys(bls_factory):
    pk = bls_factory.create_bls_keys()
    assert pk
    assert isinstance(pk, bytes)


def test_create_bls_crypto_no_keys(bls_factory):
    with pytest.raises(LoadBLSKeyError):
        bls_factory.create_bls_crypto()


def test_create_bls_crypto(bls_factory, serializer):
    pk = bls_factory.create_bls_keys()
    bls_crypto = bls_factory.create_bls_crypto()
    assert bls_crypto
    assert bls_crypto._sk
    assert bls_crypto.pk
    assert serializer.deserialize(pk) == bls_crypto.pk


def test_create_bls_keys_multiple_times(bls_factory, serializer):
    pk1 = bls_factory.create_bls_keys()
    bls_crypto1 = bls_factory.create_bls_crypto()
    assert serializer.deserialize(pk1) == bls_crypto1.pk

    pk2 = bls_factory.create_bls_keys()
    bls_crypto2 = bls_factory.create_bls_crypto()
    assert serializer.deserialize(pk2) == bls_crypto2.pk

    pk3 = bls_factory.create_bls_keys()
    bls_crypto3 = bls_factory.create_bls_crypto()
    assert serializer.deserialize(pk3) == bls_crypto3.pk


def test_bls_crypto_works(bls_factory, bls_factory2):
    # create bls for Node1
    bls_factory.create_bls_keys()
    bls_crypto1 = bls_factory.create_bls_crypto()
    pk1 = bls_crypto1.pk

    # create bls for Node2
    bls_factory2.create_bls_keys()
    bls_crypto2 = bls_factory2.create_bls_crypto()
    pk2 = bls_crypto2.pk

    # each node signs the message
    msg = 'Hello!'
    pks = [pk1, pk2]
    sigs = []
    sigs.append(bls_crypto1.sign(msg))
    sigs.append(bls_crypto2.sign(msg))

    # each node creates multi-sig
    multi_sig1 = bls_crypto1.create_multi_sig(sigs)
    multi_sig2 = bls_crypto2.create_multi_sig(sigs)

    # each node verifies multi-sigs
    assert bls_crypto1.verify_multi_sig(multi_sig1, msg, pks)
    assert bls_crypto1.verify_multi_sig(multi_sig2, msg, pks)
    assert bls_crypto2.verify_multi_sig(multi_sig1, msg, pks)
    assert bls_crypto2.verify_multi_sig(multi_sig2, msg, pks)
