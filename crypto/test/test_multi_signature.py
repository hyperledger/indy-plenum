from collections import OrderedDict

import base58
import pytest

from common.serializers.serialization import multi_signature_value_serializer
from crypto.bls.bls_multi_signature import MultiSignature, MultiSignatureValue
from plenum.common.util import get_utc_epoch

state_root_hash = base58.b58encode(b"somefakeroothashsomefakeroothash")
pool_state_root_hash = base58.b58encode(b"somefakepoolroothashsomefakepoolroothash")
txn_root_hash = base58.b58encode(b"somefaketxnroothashsomefaketxnroothash")
ledger_id = 1
timestamp = get_utc_epoch()
participants = ["Node1", "Node2", "Node3"]
signature = "somefakesignaturesomefakesignaturesomefakesignature"


@pytest.fixture()
def multi_sig_value():
    return MultiSignatureValue(ledger_id=ledger_id,
                               state_root_hash=state_root_hash,
                               pool_state_root_hash=pool_state_root_hash,
                               txn_root_hash=txn_root_hash,
                               timestamp=timestamp)


@pytest.fixture()
def multi_sig(multi_sig_value):
    return MultiSignature(
        signature=signature, participants=participants, value=multi_sig_value)


@pytest.fixture()
def expected_sig_value_dict():
    # we expected it always in sorted order by keys
    return OrderedDict([('ledger_id', ledger_id),
                        ('pool_state_root_hash', pool_state_root_hash),
                        ('state_root_hash', state_root_hash),
                        ('timestamp', timestamp),
                        ('txn_root_hash', txn_root_hash),
                        ])


@pytest.fixture()
def expected_sig_value_list():
    # we expected it in declared order
    return [ledger_id,
            state_root_hash,
            pool_state_root_hash,
            txn_root_hash,
            timestamp,
            ]


@pytest.fixture()
def expected_single_sig_value(expected_sig_value_dict):
    # we expected it always in sorted order by keys
    return multi_signature_value_serializer.serialize(expected_sig_value_dict)


@pytest.fixture()
def multi_sig_value2():
    return MultiSignatureValue(ledger_id=ledger_id,
                               state_root_hash=state_root_hash,
                               pool_state_root_hash=pool_state_root_hash,
                               txn_root_hash=txn_root_hash,
                               timestamp=timestamp)


def test_valid_value(multi_sig_value):
    assert multi_sig_value


def test_invalid_value_ledger_id():
    with pytest.raises(AssertionError):
        MultiSignatureValue(ledger_id=None,
                            state_root_hash=state_root_hash,
                            pool_state_root_hash=pool_state_root_hash,
                            txn_root_hash=txn_root_hash,
                            timestamp=timestamp)


def test_invalid_value_state_root_hash():
    with pytest.raises(AssertionError):
        MultiSignatureValue(ledger_id=ledger_id,
                            state_root_hash=None,
                            pool_state_root_hash=pool_state_root_hash,
                            txn_root_hash=txn_root_hash,
                            timestamp=timestamp)


def test_invalid_value_pool_state_root_hash():
    with pytest.raises(AssertionError):
        MultiSignatureValue(ledger_id=ledger_id,
                            state_root_hash=state_root_hash,
                            pool_state_root_hash=None,
                            txn_root_hash=txn_root_hash,
                            timestamp=timestamp)


def test_invalid_value_txn_root_hash():
    with pytest.raises(AssertionError):
        MultiSignatureValue(ledger_id=ledger_id,
                            state_root_hash=state_root_hash,
                            pool_state_root_hash=pool_state_root_hash,
                            txn_root_hash=None,
                            timestamp=timestamp)


def test_invalid_value_timestamp():
    with pytest.raises(AssertionError):
        MultiSignatureValue(ledger_id=ledger_id,
                            state_root_hash=state_root_hash,
                            pool_state_root_hash=pool_state_root_hash,
                            txn_root_hash=txn_root_hash,
                            timestamp=None)


def test_value_as_dict(multi_sig_value, expected_sig_value_dict):
    assert expected_sig_value_dict == multi_sig_value.as_dict()


def test_value_as_single(multi_sig_value, expected_single_sig_value):
    assert expected_single_sig_value == multi_sig_value.as_single_value()


def test_value_equal(multi_sig_value, multi_sig_value2):
    assert multi_sig_value == multi_sig_value2


def test_single_value_equal(multi_sig_value, multi_sig_value2):
    assert multi_sig_value.as_single_value() == multi_sig_value2.as_single_value()


def test_value_as_list(multi_sig_value, expected_sig_value_list):
    assert expected_sig_value_list == multi_sig_value.as_list()


def test_value_create_from_as_dict(multi_sig_value):
    new_multi_sig_value = MultiSignatureValue(**(multi_sig_value.as_dict()))
    assert multi_sig_value == new_multi_sig_value


def test_value_create_from_as_list(multi_sig_value):
    new_multi_sig_value = MultiSignatureValue(*(multi_sig_value.as_list()))
    assert multi_sig_value == new_multi_sig_value


def test_valid_multi_sig(multi_sig):
    assert multi_sig


def test_invalid_participants():
    with pytest.raises(AssertionError):
        MultiSignature(signature=signature, participants=[], value=multi_sig_value)
    with pytest.raises(AssertionError):
        MultiSignature(signature=signature, participants=None, value=multi_sig_value)


def test_invalid_signature():
    with pytest.raises(AssertionError):
        MultiSignature(signature=None, participants=participants, value=multi_sig_value)


def test_invalid_value():
    with pytest.raises(AssertionError):
        MultiSignature(signature=signature, participants=participants, value=None)


def test_multi_sig_as_dict(multi_sig, expected_sig_value_dict):
    multi_sig_dict = multi_sig.as_dict()
    assert multi_sig_dict['signature'] == multi_sig.signature
    assert multi_sig_dict['participants'] == multi_sig.participants
    assert multi_sig_dict['value'] == expected_sig_value_dict


def test_multi_sig_as_list(multi_sig, expected_sig_value_list):
    multi_sig_list = multi_sig.as_list()
    assert multi_sig_list[0] == multi_sig.signature
    assert multi_sig_list[1] == multi_sig.participants
    assert multi_sig_list[2] == expected_sig_value_list
