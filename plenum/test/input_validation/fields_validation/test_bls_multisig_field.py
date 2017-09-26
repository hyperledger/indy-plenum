import base58
from plenum.common.messages.fields import BlsMultiSignatureField

validator = BlsMultiSignatureField()
root_hash = base58.b58encode(b"somefakeroothashsomefakeroothash")
participants = ["Node1", "Node2", "Node3"]
signature = "somefakesignaturesomefakesignaturesomefakesignature"


def test_valid():
    assert not validator.validate((signature, participants, root_hash))


def test_invalid_participants():
    assert validator.validate((signature, "[]", root_hash))
    assert validator.validate((signature, None, root_hash))
    assert validator.validate((signature, [1], root_hash))


def test_invalid_signature():
    assert validator.validate(("", participants, root_hash))
    assert validator.validate((None, participants, root_hash))
    assert validator.validate((123, participants, root_hash))


def test_invalid_root():
    assert validator.validate((signature, participants, ""))
    assert validator.validate((signature, participants, None))
    assert validator.validate((signature, participants, 3))
