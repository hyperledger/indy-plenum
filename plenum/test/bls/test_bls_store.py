import pytest

from plenum.bls.bls_store import BlsStore
from plenum.common.config_util import getConfig

config = getConfig()

sign = "1q" * 16
root_hash = "2w" * 16
participants = ["q" * 32, "w" * 32, "e" * 32, "r" * 32]

@pytest.fixture()
def bls_store(tempdir):
    bls_str = BlsStore(config.stateSignatureStorage, tempdir, config.stateSignatureDbName)
    yield bls_str
    bls_str.close()


def test_create_store(bls_store):
    pass


def test_put_to_store(bls_store):
    bls_store.put(root_hash, sign, participants)


def test_get_valid_form_store(bls_store):
    bls_store.put(root_hash, sign, participants)
    vsig, vpart = bls_store.get(root_hash)
    assert vsig == sign
    assert vpart == participants


def test_change_in_store(bls_store):
    bls_store.put(root_hash, sign, participants)
    newsig = "x" * 15
    newpart = ['9' * 5, '8' * 4]
    bls_store.put(root_hash, newsig, newpart)
    vsig, vpart = bls_store.get(root_hash)
    assert vsig == newsig
    assert vpart == newpart


def test_get_invalid_form_store(bls_store):
    bls_store.put(root_hash, sign, participants)
    vsig, vpart = bls_store.get("invalid")
    assert vsig is None
    assert vpart is None
