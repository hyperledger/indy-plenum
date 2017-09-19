from plenum.common.messages.fields import BlsMultiSignatureField
import base58

validator = BlsMultiSignatureField()
root_hash = base58.b58encode(b"somefakeroothashsomefakeroothash")
participants = ["Node1", "Node2", "Node3"]
signature = "somefakesignaturesomefakesignaturesomefakesignature"


def test_valid():
    assert not validator.validate((root_hash, participants, signature))


def test_invalid_participants():
    assert validator.validate((root_hash, "[]", signature))
    assert validator.validate((root_hash, None, signature))
    assert validator.validate((root_hash, [1], signature))


def test_invalid_signature():
    assert validator.validate((root_hash, participants, ""))
    assert validator.validate((root_hash, participants, None))
    assert validator.validate((root_hash, participants, 123))


def test_invalid_root():
    assert validator.validate(("", participants, signature))
    assert validator.validate((None, participants, signature))
    assert validator.validate((3, participants, signature))
