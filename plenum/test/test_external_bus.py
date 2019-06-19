from unittest.mock import Mock

from plenum.common.external_bus import ExternalBus
from plenum.test.test_event_bus import create_some_message, SomeMessage


def test_external_bus_forwards_sent_messages_to_send_handler():
    message = create_some_message()
    send_handler = Mock()

    bus = ExternalBus(send_handler)
    bus.send(message)

    send_handler.assert_called_once_with(message, None)


def test_external_bus_forwards_addressed_messages_to_send_handler():
    message = create_some_message()
    send_handler = Mock()

    bus = ExternalBus(send_handler)
    bus.send(message, 'some_node')

    send_handler.assert_called_once_with(message, 'some_node')


def test_external_bus_forwards_received_messages_to_subscribers():
    message = create_some_message()
    handler = Mock()

    bus = ExternalBus(Mock())
    bus.subscribe(SomeMessage, handler)
    bus.recv(message, 'other_node')

    handler.assert_called_once_with(message, 'other_node')
