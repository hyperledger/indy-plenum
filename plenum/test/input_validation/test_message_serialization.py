from plenum.common.constants import CURRENT_PROTOCOL_VERSION

from plenum.common.messages.node_messages import LedgerStatus, MessageRep
from plenum.common.types import f
from stp_zmq.zstack import ZStack


def test_that_service_fields_not_being_serialized():
    """
    Checks that service fields of validators, like 'typename' and 'schema' ]
    are excluded from serialized message
    """

    message = LedgerStatus(
        1, 10, None, None, "AwgQhPR9cgRubttBGjRruCRMLhZFBffbejbPipj7WBBm", CURRENT_PROTOCOL_VERSION)
    serialized = ZStack.serializeMsg(message)
    deserialized = ZStack.deserializeMsg(serialized)
    service_fields = {'typename', 'schema', 'optional', 'nullable'}
    assert service_fields - set(deserialized) == service_fields


def test_that_dir_returns_only_message_keys():
    message = LedgerStatus(
        1, 10, None, None, "AwgQhPR9cgRubttBGjRruCRMLhZFBffbejbPipj7WBBm", CURRENT_PROTOCOL_VERSION)
    assert set(dir(message)) == set(message.keys())


def test_serialization_of_submessages_to_dict():
    message = LedgerStatus(
        1, 10, None, None, "AwgQhPR9cgRubttBGjRruCRMLhZFBffbejbPipj7WBBm", CURRENT_PROTOCOL_VERSION)
    message_rep = MessageRep(**{
        f.MSG_TYPE.nm: "LEDGER_STATUS",
        f.PARAMS.nm: {"ledger_id": 1, f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION},
        f.MSG.nm: message,
    })
    serialized_message = ZStack.serializeMsg(message).decode()
    serialized_message_reply = ZStack.serializeMsg(message_rep).decode()

    # check that submessage (LedgerStatus) is serialized to the same dict as
    # it were a common message
    assert serialized_message in serialized_message_reply

    # check that de-serialized into the same message
    deserialized_message = LedgerStatus(
        **ZStack.deserializeMsg(serialized_message))
    deserialized_submessage = LedgerStatus(
        **ZStack.deserializeMsg(serialized_message_reply)[f.MSG.nm])
    assert message == deserialized_message
    assert message_rep.msg == deserialized_submessage
    assert message == deserialized_submessage
