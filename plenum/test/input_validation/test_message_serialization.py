from plenum.common.messages.node_messages import LedgerStatus
from stp_zmq.zstack import ZStack


def test_that_service_fields_not_being_serialized():
    """
    Checks that service fields of validators, like 'typename' and 'schema' ]
    are excluded from serialized message
    """

    message = LedgerStatus(1,10,None,None,"AwgQhPR9cgRubttBGjRruCRMLhZFBffbejbPipj7WBBm")
    serialized = ZStack.serializeMsg(message)
    deserialized = ZStack.deserializeMsg(serialized)
    service_fields = {'typename', 'schema', 'optional', 'nullable'}
    assert service_fields - set(deserialized) == service_fields


def test_that_dir_returns_only_message_keys():
    message = LedgerStatus(1, 10, None, None, "AwgQhPR9cgRubttBGjRruCRMLhZFBffbejbPipj7WBBm")
    assert set(dir(message)) == set(message.keys())
