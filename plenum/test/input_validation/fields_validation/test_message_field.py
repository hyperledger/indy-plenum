import pytest
from plenum.common.messages.fields import MessageField
from plenum.common.messages.node_messages import CommitMsgData, ViewChangeDoneMsgData


def test_correct_message():
    validator = MessageField(CommitMsgData)
    message = CommitMsgData(1, 2, 3)
    assert not validator.validate(message)


def test_incorrect_message():
    validator = MessageField(ViewChangeDoneMsgData)
    message = CommitMsgData(1, 2, 3)
    assert validator.validate(message)
