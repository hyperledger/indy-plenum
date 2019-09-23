from plenum.common.messages.fields import BatchIDField

validator = BatchIDField()


def test_valid():
    assert not validator.validate((0, 1, 1, "digest"))
    assert not validator.validate((10, 10, 1, "digest"))
    assert not validator.validate((100, 90, 0, "d"))
    assert not validator.validate({'view_no': 100,
                                   'pp_view_no': 90,
                                   'pp_seq_no': 0,
                                   'pp_digest': "d"})


def test_invalid_view_no():
    assert validator.validate((-1, 1, 1, "digest"))
    assert validator.validate(("aaa", 1, 1, "digest"))


def test_invalid_pp_view_no():
    assert validator.validate((1, -1, 1, "digest"))
    assert validator.validate((1, "aaa", 1, "digest"))


def test_invalid_pp_seq_no():
    assert validator.validate((1, 1, -1, "digest"))
    assert validator.validate((1, 1, "aaa", "digest"))


def test_invalid_digest():
    assert validator.validate((1, 1, 1, ""))
    assert validator.validate((1, 1, 1, 1))


def test_invalid_keys():
    assert validator.validate({'pp_view_no': 90,
                               'pp_seq_no': 0,
                               'pp_digest': "d"})
    assert validator.validate({'unknown_field': 100,
                               'pp_view_no': 90,
                               'pp_seq_no': 0,
                               'pp_digest': "d"})
