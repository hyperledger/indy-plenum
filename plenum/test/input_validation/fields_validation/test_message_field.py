import pytest
from plenum.common.messages.fields import MessageField
from plenum.common.messages.node_messages import Commit, ViewChangeDone


def test_correct_message():
    validator = MessageField(Commit)
    message = Commit(1, 2, 3)
    assert not validator.validate(message)


def test_incorrect_message():
    validator = MessageField(ViewChangeDone)
    message = Commit(1, 2, 3)
    assert validator.validate(message)
