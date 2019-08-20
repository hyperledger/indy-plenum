from plenum.common.messages.fields import BatchIDField

validator = BatchIDField()


def test_valid():
    assert not validator.validate((1, 1, "digest"))
    assert not validator.validate((0, 1, "digest"))
    assert not validator.validate((100, 0, "d"))


def test_invalid_view_no():
    assert validator.validate((-1, 1, "digest"))
    assert validator.validate(("aaa", 1, "digest"))


def test_invalid_pp_seq_no():
    assert validator.validate((1, -1, "digest"))
    assert validator.validate((1, "aaa", "digest"))


def test_invalid_digest():
    assert validator.validate((1, 1, ""))
    assert validator.validate((1, 1, 1))
