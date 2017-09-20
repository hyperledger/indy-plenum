import base58
import pytest
from crypto.bls.bls_multi_signature import MultiSignature

root_hash = base58.b58encode(b"somefakeroothashsomefakeroothash")
participants = ["Node1", "Node2", "Node3"]
signature = "somefakesignaturesomefakesignaturesomefakesignature"


def test_valid():
    assert MultiSignature(
        signature=signature, participants=participants, pool_state_root=root_hash)


def test_invalid_participants():
    with pytest.raises(AssertionError):
        MultiSignature(signature=signature, participants=[], pool_state_root=root_hash)
    with pytest.raises(AssertionError):
        MultiSignature(signature=signature, participants=None, pool_state_root=root_hash)


def test_invalid_signature():
    with pytest.raises(AssertionError):
        MultiSignature(signature=None, participants=participants, pool_state_root=root_hash)


def test_invalid_root():
    with pytest.raises(AssertionError):
        MultiSignature(signature=signature, participants=participants, pool_state_root=None)
