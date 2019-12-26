from random import randint, random, choice
from typing import NamedTuple
from unittest.mock import Mock, call

from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.util import randomString

SomeMessage = NamedTuple('SomeMessage', [('int_field', int), ('str_field', str)])
OtherMessage = NamedTuple('OtherMessage', [('float_field', float)])
NoParamsMessage = NamedTuple('NoParamsMessage', [])
OtherNoParamsMessage = NamedTuple('OtherNoParamsMessage', [])


def create_some_message() -> SomeMessage:
    return SomeMessage(int_field=randint(0, 1000), str_field=randomString(16))


def create_other_message() -> OtherMessage:
    return OtherMessage(float_field=random())


def test_event_bus_routes_registered_message():
    message = create_some_message()
    handler = Mock()

    bus = InternalBus()
    bus.subscribe(SomeMessage, handler)
    bus.send(message)

    handler.assert_called_once_with(message)


def test_event_bus_routes_no_params_message():
    message = NoParamsMessage()
    handler = Mock()

    bus = InternalBus()
    bus.subscribe(NoParamsMessage, handler)
    bus.send(message)

    handler.assert_called_once_with(message)


def test_event_bus_understands_different_no_params_messages():
    a = NoParamsMessage()
    b = OtherNoParamsMessage()
    handler_a = Mock()
    handler_b = Mock()

    bus = InternalBus()
    bus.subscribe(NoParamsMessage, handler_a)
    bus.subscribe(OtherNoParamsMessage, handler_b)

    bus.send(b)
    handler_a.assert_not_called()
    handler_b.assert_called_once_with(b)

    bus.send(a)
    handler_a.assert_called_once_with(a)
    handler_b.assert_called_once_with(b)


def test_internal_bus_doesnt_route_unregistered_message():
    handler = Mock()

    bus = InternalBus()
    bus.subscribe(SomeMessage, handler)
    bus.send(create_other_message())

    handler.assert_not_called()


def test_internal_bus_routes_messages_to_all_subscribers():
    message = create_some_message()
    handler1 = Mock()
    handler2 = Mock()

    bus = InternalBus()
    bus.subscribe(SomeMessage, handler1)
    bus.subscribe(SomeMessage, handler2)
    bus.send(message)

    handler1.assert_called_once_with(message)
    handler2.assert_called_once_with(message)


def test_internal_bus_sequentially_routes_multiple_messages_of_different_types():
    all_messages = [create_some_message() if random() < 0.5 else create_other_message() for _ in range(100)]
    some_handler1 = Mock()
    some_handler2 = Mock()
    other_handler = Mock()

    bus = InternalBus()
    bus.subscribe(SomeMessage, some_handler1)
    bus.subscribe(SomeMessage, some_handler2)
    bus.subscribe(OtherMessage, other_handler)
    for message in all_messages:
        bus.send(message)

    assert some_handler1.mock_calls == [call(msg) for msg in all_messages if isinstance(msg, SomeMessage)]
    assert some_handler2.mock_calls == [call(msg) for msg in all_messages if isinstance(msg, SomeMessage)]
    assert other_handler.mock_calls == [call(msg) for msg in all_messages if isinstance(msg, OtherMessage)]


def test_internal_bus_can_route_messages_with_side_arguments():
    message = create_some_message()
    handler = Mock()

    bus = InternalBus()
    bus.subscribe(SomeMessage, handler)
    bus.send(message, 'some_arg', 'other_arg')

    handler.assert_called_once_with(message, 'some_arg', 'other_arg')


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


def test_external_bus_queues_sent_messages_sequentially():
    messages = [(create_some_message(), choice(['some_node', 'other_node', None]))
                for _ in range(100)]
    send_handler = Mock()

    bus = ExternalBus(send_handler)
    for message, dst in messages:
        bus.send(message, dst)

    assert send_handler.mock_calls == [call(msg, dst) for msg, dst in messages]


def test_external_bus_forwards_received_messages_to_subscribers():
    message = create_some_message()
    handler = Mock()

    bus = ExternalBus(Mock())
    bus.subscribe(SomeMessage, handler)
    bus.process_incoming(message, 'other_node')

    handler.assert_called_once_with(message, 'other_node')


def test_external_bus_notifies_about_connection_changes():
    connected = Mock()
    disconnected = Mock()

    bus = ExternalBus(Mock())
    bus.subscribe(bus.Connected, connected)
    bus.subscribe(bus.Disconnected, disconnected)

    # Initial state
    assert bus.connecteds == set()
    connected.assert_not_called()
    disconnected.assert_not_called()

    # Add connections
    bus.update_connecteds({'a', 'b'})
    assert bus.connecteds == {'a', 'b'}
    connected.assert_has_calls([call(bus.Connected(), 'a'),
                                call(bus.Connected(), 'b')],
                               any_order=True)
    disconnected.assert_not_called()
    connected.reset_mock()

    # Add more connections
    bus.update_connecteds({'a', 'b', 'c'})
    assert bus.connecteds == {'a', 'b', 'c'}
    connected.assert_called_once_with(bus.Connected(), 'c')
    disconnected.assert_not_called()
    connected.reset_mock()

    # Change connections
    bus.update_connecteds({'b', 'd', 'e'})
    assert bus.connecteds == {'b', 'd', 'e'}
    connected.assert_has_calls([call(bus.Connected(), 'd'),
                                call(bus.Connected(), 'e')],
                               any_order=True)
    disconnected.assert_has_calls([call(bus.Disconnected(), 'a'),
                                   call(bus.Disconnected(), 'c')],
                                  any_order=True)
    connected.reset_mock()
    disconnected.reset_mock()

    # Disconnect everything and reconnect to a
    bus.update_connecteds({'a'})
    assert bus.connecteds == {'a'}
    connected.assert_called_once_with(bus.Connected(), 'a')
    disconnected.assert_has_calls([call(bus.Disconnected(), 'b'),
                                   call(bus.Disconnected(), 'd'),
                                   call(bus.Disconnected(), 'e')],
                                  any_order=True)
    connected.reset_mock()
    disconnected.reset_mock()

    # Disconnect
    bus.update_connecteds(set())
    assert bus.connecteds == set()
    connected.assert_not_called()
    disconnected.assert_called_once_with(bus.Disconnected(), 'a')
    disconnected.reset_mock()

    # Disconnect again
    bus.update_connecteds(set())
    assert bus.connecteds == set()
    connected.assert_not_called()
    disconnected.assert_not_called()
