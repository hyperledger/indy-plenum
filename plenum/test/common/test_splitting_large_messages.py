from plenum.common.batched import Batched
from plenum.test.testing_utils import FakeSomething

import pytest


@pytest.fixture()
def message_size_limit():
    return 10


@pytest.fixture()
def batched(message_size_limit):
    b = Batched(FakeSomething(MSG_LEN_LIMIT=message_size_limit))
    b.sign_and_serialize = lambda msg, signer: msg
    return b


def test_splitting_large_messages(batched, message_size_limit):
    """
    Checks that large message can be split by transport on smaller parts
    """
    splitter = lambda x: (x[0:len(x) // 2], x[len(x) // 2:])
    message = "!" * (message_size_limit * 3)
    parts, error = batched.prepare_for_sending(message, None, splitter)
    assert error is None
    assert len(parts) == 4
    assert "".join(parts) == message


def test_not_splitting_of_small_messages(batched, message_size_limit):
    """
    Checks that large message can be split by transport on smaller parts
    """
    message = "!" * message_size_limit
    parts, error = batched.prepare_for_sending(message, None)
    assert error is None
    assert parts == [message]


def test_fail_if_message_can_not_be_split(batched, message_size_limit):
    """
    Checks that if large message cannot be split by transport on smaller parts
    error message returned
    """
    message = "!" * (message_size_limit + 1)
    parts, error = batched.prepare_for_sending(message, None)
    assert error is not None
    assert parts is None
