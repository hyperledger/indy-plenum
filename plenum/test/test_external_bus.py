from random import choice
from unittest.mock import Mock

from plenum.common.external_bus import ExternalBus
from plenum.test.test_event_bus import create_some_message, SomeMessage


def test_external_bus_forwards_sent_messages_to_send_handler():
    message = create_some_message()

    bus = ExternalBus()
    bus.send(message)

    assert bus.outgoing == [(message, None)]


def test_external_bus_forwards_addressed_messages_to_send_handler():
    message = create_some_message()

    bus = ExternalBus()
    bus.send(message, 'some_node')

    assert bus.outgoing == [(message, 'some_node')]


def test_external_bus_queues_sent_messages_sequentially():
    messages = [(create_some_message(), choice(['some_node', 'other_node', None]))
                 for _ in range(100)]

    bus = ExternalBus()
    for message, dst in messages:
        bus.send(message, dst)

    assert bus.outgoing == messages


def test_external_bus_forwards_received_messages_to_subscribers():
    message = create_some_message()
    handler = Mock()

    bus = ExternalBus()
    bus.subscribe(SomeMessage, handler)
    bus.process_incoming(message, 'other_node')

    handler.assert_called_once_with(message, 'other_node')
