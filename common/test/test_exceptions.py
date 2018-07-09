from common.exceptions import (
    PlenumError,
    PlenumTypeError,
    PlenumValueError,
    ValueUndefinedError,
    PlenumTransportError,
    PlenumMultiIdentError,
    TooBigMessage,
    IdentityIsUnknown,
    NoSocketForIdentity,
    EAgainError
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


def test_PlenumTransportError():
    exc = PlenumTransportError('error', msg='test', ident=123)
    assert isinstance(exc, PlenumError)
    assert exc.msg == 'test'
    assert exc.ident == 123


def test_PlenumMultiIdentError():
    errors = [PlenumTransportError("err", ident=i) for i in range(10)]
    exc = PlenumMultiIdentError(errors, 'error', msg='test', ident=123)
    assert isinstance(exc, PlenumTransportError)
    assert exc.errors is errors
    assert exc.msg is None
    assert exc.ident is None


def test_TooBigMessage():
    msg = '1234567890'
    max_dump_len = 8
    max_len = 20
    msg_len = len(msg)
    exc = TooBigMessage(msg=msg, msg_len=msg_len,
                        max_len=max_len, max_dump_len=max_dump_len)
    assert isinstance(exc, PlenumTransportError)
    assert exc.msg_len == msg_len
    assert exc.max_len == max_len
    assert exc.msg_len == msg_len
    assert exc.max_dump_len == max_dump_len
    assert str(exc)[-max_dump_len:] == msg[:max_dump_len]
    assert ("Message is too big: msg_len {}, max_len {} msg {}"
            .format(msg_len, max_len, msg[:max_dump_len])) == str(exc)


def test_IdentityIsUnknown():
    ident = 123
    exc = IdentityIsUnknown(ident=ident)
    assert isinstance(exc, PlenumTransportError)
    assert str(exc) == "Identity {} is unknown".format(ident)


def test_NoSocketForIdentity():
    ident = 123
    exc = NoSocketForIdentity(ident=ident)
    assert isinstance(exc, PlenumTransportError)
    assert str(exc) == ("Socket for identity {} is not initialized"
                        .format(ident))


def test_EAgainError():
    msg = "test"
    ident = 123
    exc = EAgainError(msg=msg, ident=ident)
    assert isinstance(exc, PlenumTransportError)
    assert str(exc) == ("The message cannot be sent at the "
                        "moment: identity {}, msg {}.".format(ident, msg))
