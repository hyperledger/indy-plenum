import pytest

from crypto.bls.bls_multi_signature import MultiSignature
from plenum.bls.bls_store import BlsStore
from plenum.common.config_util import getConfig
from common.serializers.serialization import multi_sig_store_serializer


config = getConfig()

sign = "1q" * 16
root_hash = "2w" * 16
participants = ["q" * 32, "w" * 32, "e" * 32, "r" * 32]
pool_root_hash = "3w" * 16
multi_sig = MultiSignature(sign, participants, pool_root_hash)


@pytest.fixture()
def bls_store(tempdir):
    bls_str = BlsStore(config.stateSignatureStorage,
                       tempdir,
                       config.stateSignatureDbName,
                       multi_sig_store_serializer)
    yield bls_str
    bls_str.close()


def test_create_store(bls_store):
    pass


def test_put_to_store(bls_store):
    bls_store.put(root_hash, multi_sig)


def test_get_valid_form_store(bls_store):
    bls_store.put(root_hash, multi_sig)
    vsig = bls_store.get(root_hash)
    assert vsig == multi_sig


def test_change_in_store(bls_store):
    bls_store.put(root_hash, multi_sig)

    newsig = MultiSignature(
        signature="x" * 15,
        participants=['9' * 5, '8' * 4],
        pool_state_root=multi_sig.pool_state_root
    )

    bls_store.put(root_hash, newsig)
    vsig = bls_store.get(root_hash)
    assert vsig == newsig


def test_get_invalid_form_store(bls_store):
    bls_store.put(root_hash, multi_sig)
    vsig = bls_store.get("invalid")
    assert vsig is None
