import os
import pytest
from crypto.bls.bls_bft import BlsBft
from crypto.bls.bls_bft_replica import BlsBftReplica
from crypto.bls.bls_key_register import BlsKeyRegister
from plenum.bls.bls_bft_factory import BlsFactoryBftPlenum
from plenum.bls.bls_crypto_factory import create_default_bls_crypto_factory

from plenum.bls.bls_store import BlsStore

i = 0


@pytest.fixture()
def node(txnPoolNodeSet):
    return txnPoolNodeSet[0]


@pytest.fixture()
def bls_bft_factory(node, tdir_for_func):
    dir = os.path.join(tdir_for_func, 'TestNode')
    bls_crypto_factory = create_default_bls_crypto_factory(dir)
    return BlsFactoryBftPlenum(bls_crypto_factory,
                               node)


@pytest.fixture()
def test_bls_store(node):
    node.config.stateSignatureDbName = 'TestBlsStore{}'.format(i)


def test_create_bls_bft(bls_bft_factory, test_bls_store):
    bls_bft_factory._bls_factory_crypto.generate_and_store_bls_keys()
    bls_bft = bls_bft_factory.create_bls_bft()
    assert bls_bft
    assert isinstance(bls_bft, BlsBft)
    assert bls_bft.can_sign_bls()


def test_create_bls_bft_no_keys(bls_bft_factory, test_bls_store):
    bls_bft = bls_bft_factory.create_bls_bft()
    assert bls_bft
    assert isinstance(bls_bft, BlsBft)
    assert not bls_bft.can_sign_bls()


def test_create_bls_bft_replica(bls_bft_factory):
    bls_bft_replica = bls_bft_factory.create_bls_bft_replica(is_master=True)
    assert bls_bft_replica
    assert isinstance(bls_bft_replica, BlsBftReplica)


def test_create_bls_store(bls_bft_factory, test_bls_store):
    bls_store = bls_bft_factory.create_bls_store()
    assert bls_store
    assert isinstance(bls_store, BlsStore)


def test_create_bls_key_registry(bls_bft_factory):
    bls_key_register = bls_bft_factory.create_bls_key_register()
    assert bls_key_register
    assert isinstance(bls_key_register, BlsKeyRegister)
