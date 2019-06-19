from random import randint, random
from typing import NamedTuple
from unittest.mock import Mock, call

from plenum.common.event_bus import EventBus
from plenum.common.util import randomString

SomeMessage = NamedTuple('SomeMessage', [('int_field', int), ('str_field', str)])
OtherMessage = NamedTuple('OtherMessage', [('float_field', float)])


def create_some_message() -> SomeMessage:
    return SomeMessage(int_field=randint(0, 1000), str_field=randomString(16))


def create_other_message() -> OtherMessage:
    return OtherMessage(float_field=random())


def test_event_bus_routes_registered_message():
    message = create_some_message()
    handler = Mock()

    bus = EventBus()
    bus.subscribe(SomeMessage, handler)
    bus.send(message)

    handler.assert_called_once_with(message)


def test_event_bus_doesnt_route_unregistered_message():
    handler = Mock()

    bus = EventBus()
    bus.subscribe(SomeMessage, handler)
    bus.send(create_other_message())

    handler.assert_not_called()


def test_event_bus_routes_messages_to_all_subscribers():
    message = create_some_message()
    handler1 = Mock()
    handler2 = Mock()

    bus = EventBus()
    bus.subscribe(SomeMessage, handler1)
    bus.subscribe(SomeMessage, handler2)
    bus.send(message)

    handler1.assert_called_once_with(message)
    handler2.assert_called_once_with(message)


def test_event_bus_sequentially_routes_multiple_messages_of_different_types():
    all_messages = [create_some_message() if random() < 0.5 else create_other_message() for _ in range(100)]
    some_handler1 = Mock()
    some_handler2 = Mock()
    other_handler = Mock()

    bus = EventBus()
    bus.subscribe(SomeMessage, some_handler1)
    bus.subscribe(SomeMessage, some_handler2)
    bus.subscribe(OtherMessage, other_handler)
    for message in all_messages:
        bus.send(message)

    assert some_handler1.mock_calls == [call(msg) for msg in all_messages if isinstance(msg, SomeMessage)]
    assert some_handler2.mock_calls == [call(msg) for msg in all_messages if isinstance(msg, SomeMessage)]
    assert other_handler.mock_calls == [call(msg) for msg in all_messages if isinstance(msg, OtherMessage)]


def test_event_bus_can_route_messages_with_side_arguments():
    message = create_some_message()
    handler = Mock()

    bus = EventBus()
    bus.subscribe(SomeMessage, handler)
    bus.send(message, 'some_arg', 'other_arg')

    handler.assert_called_once_with(message, 'some_arg', 'other_arg')
