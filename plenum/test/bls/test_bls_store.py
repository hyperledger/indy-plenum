import copy

import base58
import pytest

from common.exceptions import ValueUndefinedError
from crypto.bls.bls_multi_signature import MultiSignature, MultiSignatureValue
from plenum.bls.bls_store import BlsStore
from plenum.common.util import get_utc_epoch
import plenum.config as plenum_config

state_root_hash = base58.b58encode(b"somefakeroothashsomefakeroothash").decode("utf-8")
pool_state_root_hash = base58.b58encode(b"somefakepoolroothashsomefakepoolroothash").decode("utf-8")
txn_root_hash = base58.b58encode(b"somefaketxnroothashsomefaketxnroothash").decode("utf-8")
ledger_id = 1
timestamp = get_utc_epoch()

value = MultiSignatureValue(ledger_id=ledger_id,
                            state_root_hash=state_root_hash,
                            pool_state_root_hash=pool_state_root_hash,
                            txn_root_hash=txn_root_hash,
                            timestamp=timestamp)

sign = "1q" * 16
participants = ["q" * 32, "w" * 32, "e" * 32, "r" * 32]

multi_sig = MultiSignature(sign, participants, value)


@pytest.fixture()
def bls_store(tdir_for_func):
    bls_str = BlsStore(plenum_config.stateSignatureStorage,
                       tdir_for_func,
                       plenum_config.stateSignatureDbName)
    yield bls_str
    bls_str.close()


def test_create_store(bls_store):
    pass


def test_put_undefined_sig_to_store(bls_store):
    with pytest.raises(ValueUndefinedError):
        bls_store.put(None)


def test_put_to_store(bls_store, fake_multi_sig):
    bls_store.put(fake_multi_sig)


def test_get_valid_form_store(bls_store, fake_multi_sig, fake_state_root_hash):
    bls_store.put(fake_multi_sig)
    vsig = bls_store.get(fake_state_root_hash)
    assert vsig == fake_multi_sig


def test_change_in_store(bls_store, fake_multi_sig, fake_state_root_hash):
    bls_store.put(fake_multi_sig)

    new_fake_multi_sig = copy.deepcopy(fake_multi_sig)
    new_fake_multi_sig.signature = "x" * 15
    new_fake_multi_sig.participants = ['9' * 5, '8' * 4]

    bls_store.put(new_fake_multi_sig)
    vsig = bls_store.get(fake_state_root_hash)
    assert vsig == new_fake_multi_sig


def test_get_invalid_form_store(bls_store, fake_multi_sig):
    bls_store.put(fake_multi_sig)
    vsig = bls_store.get("invalid")
    assert vsig is None
