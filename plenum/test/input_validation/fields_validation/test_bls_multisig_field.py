from plenum.common.messages.fields import BlsMultiSignatureField

validator = BlsMultiSignatureField()


def test_valid():
    data = (["Node1", "Node2", "Node3"], "fffffffffffff")
    assert not validator.validate(data)


def test_invalid_participants():
    data = ([], "fffffffffffff")
    assert validator.validate(data)
    data = (None, "fffffffffffff")
    assert validator.validate(data)
    data = ([1], "fffffffffffff")
    assert validator.validate(data)


def test_invalid_signature():
    data = (["Node1", "Node2", "Node3"], "")
    assert validator.validate(data)
    data = (["Node1", "Node2", "Node3"], None)
    assert validator.validate(data)
    data = (["Node1", "Node2", "Node3"], 123)
    assert validator.validate(data)
