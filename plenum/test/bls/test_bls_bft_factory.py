import pytest
from crypto.bls.bls_bft import BlsBft
from crypto.bls.bls_key_manager import LoadBLSKeyError
from plenum.bls.bls_bft_factory import BlsFactoryBftPlenum
from plenum.bls.bls_crypto_factory import create_default_bls_crypto_factory

from plenum.bls.bls_store import BlsStore
from plenum.common.config_util import getConfig

config = getConfig()


@pytest.fixture()
def node(txnPoolNodeSet):
    return txnPoolNodeSet[0]


@pytest.fixture()
def bls_bft_factory(node, tempdir):
    bls_crypto_factory = create_default_bls_crypto_factory(tempdir,
                                                           'TestNode')
    return BlsFactoryBftPlenum(bls_crypto_factory,
                               node)


def test_create_bls_bft(bls_bft_factory):
    bls_bft_factory._bls_factory_crypto.generate_and_store_bls_keys()
    bls_bft = bls_bft_factory.create_bls_bft(is_master=True)
    assert bls_bft
    assert isinstance(bls_bft, BlsBft)


def test_create_bls_bft_crypto_no_keys(bls_bft_factory):
    with pytest.raises(LoadBLSKeyError):
        bls_bft_factory.create_bls_bft(is_master=False)


def test_create_bls_store(bls_bft_factory, node):
    node.config.stateSignatureDbName = 'TestBlsStore'
    bls_store = bls_bft_factory.create_bls_store()
    assert bls_store
    assert isinstance(bls_store, BlsStore)
