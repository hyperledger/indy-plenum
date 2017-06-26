from plenum.common.messages.message_base import MessageBase
from plenum.common.messages.node_messages import node_message_factory
from plenum.common.types import TaggedTuple, NonEmptyStringField
from plenum.common.util import randomString


def randomMsg() -> TaggedTuple:
    return TestMsg('subject ' + randomString(),
                   'content ' + randomString())


TESTMSG = "TESTMSG"
TestMsg = TaggedTuple(TESTMSG, [
    ("subject", str),
    ("content", str)])


class TestMsg(MessageBase):
    typename = TESTMSG
    schema = (
        ("subject", NonEmptyStringField()),
        ("content", NonEmptyStringField()),
    )


node_message_factory.set_message_class(TestMsg)
