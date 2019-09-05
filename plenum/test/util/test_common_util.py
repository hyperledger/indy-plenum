from plenum.common.exceptions import InsufficientCorrectSignatures, InsufficientSignatures, SigningException
from plenum.common.util import randomString, friendlyEx


def test_random_string():
    """
        Check that max hex digit "f" can be achieved.
        For the task INDY-1754
    """
    for i in range(100000):
        if randomString()[-1] == 'f':
            return
    assert False


def test_friendly_exception_formatting_exc_without_str_overload():
    """
        Check if the `friendyEx` is formatting exceptions without a reason field correctly
    """
    ex = SigningException()

    formatted_exception = friendlyEx(ex)

    assert formatted_exception == '{}'.format(ex)


def test_friendly_exception_formatting_exc_with_str_overload():
    """
        Check if the `friendyEx` is formatting exceptions with a reason field correctly
    """
    ex = InsufficientSignatures(1, 3)

    formatted_exception = friendlyEx(ex)

    assert formatted_exception == '{}'.format(ex.reason)


def test_friendly_exception_formatting_multiple_exceptions():
    """
        Check if the `friendyEx` is formatting multiple exceptions with and without reason field correctly
    """
    ex1 = InsufficientCorrectSignatures(1, 2, {'6ouriXMZkLeHsuXrN1X1fd': '3GoEPiwhJUjALzrXmmE9tFTXAi7Emv8Y8jjSxQyQB'})
    ex2 = InsufficientSignatures(1, 3)
    ex2.__cause__ = ex1
    ex3 = SigningException()
    ex3.__cause__ = ex2

    expected = '{} [caused by {} [caused by {}]]'.format(ex3, ex2.reason, ex1.reason)
    formatted_exception = friendlyEx(ex3)

    assert formatted_exception == expected
