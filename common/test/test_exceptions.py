from common.exceptions import (
    PlenumError, PlenumTypeError, PlenumValueError,
    ValueUndefinedError
)


def test_PlenumError():
    exc = PlenumError()
    assert isinstance(exc, Exception)


def test_PlenumTypeError():
    aaa = 1

    exc = PlenumTypeError('aaa', aaa, str, prefix="PREFIX")
    assert isinstance(exc, PlenumError)
    assert isinstance(exc, TypeError)

    exc_str = str(exc)
    assert exc_str.startswith("PREFIX: ")
    assert "variable 'aaa'" in exc_str
    assert "type {}".format(type(aaa)) in exc_str
    assert "expected: {}".format(str) in exc_str


def test_PlenumValueError():
    aaa = 1
    expected = "= 0"

    exc = PlenumValueError('aaa', aaa, expected, prefix="PREFIX")
    assert isinstance(exc, PlenumError)
    assert isinstance(exc, ValueError)

    exc_str = str(exc)
    assert exc_str.startswith("PREFIX: ")
    assert "variable 'aaa'" in exc_str
    assert "value {}".format(aaa) in exc_str
    assert "expected: {}".format(expected) in exc_str


def test_ValueUndefinedError():
    exc = ValueUndefinedError('aaa', prefix="PREFIX")
    assert isinstance(exc, PlenumError)
    assert isinstance(exc, ValueError)

    exc_str = str(exc)
    assert exc_str.startswith("PREFIX: ")
    assert "variable 'aaa' is undefined" in exc_str
