from plenum.common.messages.fields import NonEmptyStringField
from plenum.common.messages.message_base import MessageBase, NetworkMessage
from plenum.common.messages.node_message_factory import node_message_factory
from plenum.common.util import randomString


def randomMsg():
    return TestMsg('subject ' + randomString(),
                   'content ' + randomString())


class TestMsgMsgData(MessageBase):
    typename = "TESTMSG"
    schema = (
        ("subject", NonEmptyStringField()),
        ("content", NonEmptyStringField()),
    )


class TestMsg(NetworkMessage):
    msg_data_cls = TestMsgMsgData


node_message_factory.set_message_class(TestMsgMsgData, TestMsg)
