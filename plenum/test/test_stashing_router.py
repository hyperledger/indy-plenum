from unittest.mock import Mock

from plenum.common.event_bus import InternalBus
from plenum.common.stashing_router import StashingRouter, STASH
from plenum.test.test_event_bus import SomeMessage, OtherMessage, create_some_message, create_other_message


def test_stashing_router_correctly_routes_messages():
    some_handler = Mock()
    other_handler = Mock()

    bus = InternalBus()
    router = StashingRouter(10)
    router.subscribe(SomeMessage, some_handler)
    router.subscribe(OtherMessage, other_handler)
    router.subscribe_to(bus)

    some_handler.assert_not_called()
    other_handler.assert_not_called()

    some_message = create_some_message()
    bus.send(some_message)
    some_handler.assert_called_once_with(some_message)
    other_handler.assert_not_called()

    other_message = create_other_message()
    bus.send(other_message)
    some_handler.assert_called_once_with(some_message)
    other_handler.assert_called_once_with(other_message)


def test_stashing_router_correctly_handles_multiple_arguments():
    handler = Mock()

    bus = InternalBus()
    router = StashingRouter(10)
    router.subscribe(SomeMessage, handler)
    router.subscribe_to(bus)

    message = create_some_message()
    bus.send(message, 'hello')
    handler.assert_called_once_with(message, 'hello')


def test_stashing_router_can_stash_messages():
    stash_count = 3
    calls = []

    def handler(msg):
        nonlocal stash_count
        calls.append(msg)
        if stash_count > 0:
            stash_count -= 1
            return STASH

    bus = InternalBus()
    router = StashingRouter(10)
    router.subscribe(SomeMessage, handler)
    router.subscribe_to(bus)

    msg_a = create_some_message()
    msg_b = create_some_message()
    bus.send(msg_a)
    bus.send(msg_b)
    assert calls == [msg_a, msg_b]

    router.process_stashed()
    assert calls == [msg_a, msg_b, msg_a, msg_b]

    router.process_stashed()
    assert calls == [msg_a, msg_b, msg_a, msg_b, msg_a]

    router.process_stashed()
    assert calls == [msg_a, msg_b, msg_a, msg_b, msg_a]


def test_stashing_router_can_stash_messages_with_different_reasons():
    calls = []

    def handler(message: SomeMessage):
        calls.append(message)
        if message.int_field % 2 == 0:
            return STASH + 0
        else:
            return STASH + 1

    bus = InternalBus()
    router = StashingRouter(10)
    router.subscribe(SomeMessage, handler)
    router.subscribe_to(bus)

    messages = [create_some_message() for _ in range(10)]
    for msg in messages:
        bus.send(msg)

    calls.clear()
    router.process_stashed()
    assert calls == sorted(messages, key=lambda m: m.int_field % 2)

    calls.clear()
    router.process_stashed(STASH + 0)
    assert all(msg.int_field % 2 == 0 for msg in calls)
    assert all(msg in messages for msg in calls)

    calls.clear()
    router.process_stashed(STASH + 1)
    assert all(msg.int_field % 2 != 0 for msg in calls)
    assert all(msg in messages for msg in calls)


def test_stashing_router_can_stash_and_sort_messages():
    calls = []

    def handler(message: SomeMessage):
        calls.append(message)
        return STASH

    def sort_key(message: SomeMessage):
        return message.int_field

    bus = InternalBus()
    router = StashingRouter(10)
    router.set_sorted_stasher(STASH, key=sort_key)
    router.subscribe(SomeMessage, handler)
    router.subscribe_to(bus)

    messages = [create_some_message() for _ in range(10)]
    for msg in messages:
        bus.send(msg)

    assert calls == messages

    calls.clear()
    router.process_stashed()
    assert calls == sorted(messages, key=sort_key)


def test_stashing_router_can_process_stashed_until_first_restash():
    calls = []

    def handler(msg):
        calls.append(msg)
        if len(calls) % 2 != 0:
            return STASH

    bus = InternalBus()
    router = StashingRouter(10)
    router.subscribe(SomeMessage, handler)
    router.subscribe_to(bus)

    msg_a = create_some_message()
    msg_b = create_some_message()
    msg_c = create_some_message()
    msg_d = create_some_message()
    msg_e = create_some_message()
    bus.send(msg_a)
    bus.send(msg_b)
    bus.send(msg_c)
    bus.send(msg_d)
    bus.send(msg_e)
    assert calls == [msg_a, msg_b, msg_c, msg_d, msg_e]

    # Stash contains A, C, E, going to stop on C
    router.process_stashed(stop_on_stash=True)
    assert calls == [msg_a, msg_b, msg_c, msg_d, msg_e, msg_a, msg_c]

    # Stash contains E, C, going to stop on C
    router.process_stashed(stop_on_stash=True)
    assert calls == [msg_a, msg_b, msg_c, msg_d, msg_e, msg_a, msg_c, msg_e, msg_c]

    # Stash contains C, not going to stop
    router.process_stashed(stop_on_stash=True)
    assert calls == [msg_a, msg_b, msg_c, msg_d, msg_e, msg_a, msg_c, msg_e, msg_c, msg_c]

    # Stash doesn't contain anything
    router.process_stashed(stop_on_stash=True)
    assert calls == [msg_a, msg_b, msg_c, msg_d, msg_e, msg_a, msg_c, msg_e, msg_c, msg_c]
