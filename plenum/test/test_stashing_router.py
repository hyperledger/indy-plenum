from unittest.mock import Mock

from plenum.common.event_bus import InternalBus
from plenum.common.stashing_router import StashingRouter, STASH, PROCESS, DISCARD
from plenum.test.test_event_bus import SomeMessage, OtherMessage, create_some_message, create_other_message


def test_stashing_router_correctly_routes_messages():
    some_handler = Mock(return_value=(PROCESS, ""))
    other_handler = Mock(return_value=(DISCARD, ""))

    bus = InternalBus()
    router = StashingRouter(10, buses=[bus])
    router.subscribe(SomeMessage, some_handler)
    router.subscribe(OtherMessage, other_handler)

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
    handler = Mock(return_value=(PROCESS, ""))

    bus = InternalBus()
    router = StashingRouter(10, buses=[bus])
    router.subscribe(SomeMessage, handler)

    message = create_some_message()
    bus.send(message, 'hello')
    handler.assert_called_once_with(message, 'hello')


def test_process_all_stashed_doesnt_do_anything_when_there_are_no_items_in_stash():
    handler = Mock(return_value=(PROCESS, ""))

    bus = InternalBus()
    router = StashingRouter(10, buses=[bus])
    router.subscribe(SomeMessage, handler)

    router.process_all_stashed()
    handler.assert_not_called()

    message = create_some_message()
    bus.send(message, 'hello')
    handler.assert_called_once_with(message, 'hello')

    router.process_all_stashed()
    handler.assert_called_once_with(message, 'hello')


def test_process_stashed_until_restash_doesnt_do_anything_when_there_are_no_items_in_stash():
    handler = Mock(return_value=(PROCESS, ""))

    bus = InternalBus()
    router = StashingRouter(10, buses=[bus])
    router.subscribe(SomeMessage, handler)

    router.process_stashed_until_first_restash()
    handler.assert_not_called()

    message = create_some_message()
    bus.send(message, 'hello')
    handler.assert_called_once_with(message, 'hello')

    router.process_stashed_until_first_restash()
    handler.assert_called_once_with(message, 'hello')


def test_stashing_router_can_stash_messages():
    stash_count = 3
    calls = []

    def handler(msg):
        nonlocal stash_count
        calls.append(msg)
        if stash_count > 0:
            stash_count -= 1
            return STASH, "reason"
        else:
            return None, None

    bus = InternalBus()
    router = StashingRouter(10, buses=[bus])
    router.subscribe(SomeMessage, handler)

    msg_a = create_some_message()
    msg_b = create_some_message()
    bus.send(msg_a)
    bus.send(msg_b)
    assert router.stash_size() == 2
    assert calls == [msg_a, msg_b]

    router.process_all_stashed()
    assert router.stash_size() == 1
    assert calls == [msg_a, msg_b, msg_a, msg_b]

    router.process_all_stashed()
    assert router.stash_size() == 0
    assert calls == [msg_a, msg_b, msg_a, msg_b, msg_a]

    router.process_all_stashed()
    assert router.stash_size() == 0
    assert calls == [msg_a, msg_b, msg_a, msg_b, msg_a]


def test_stashing_router_can_stash_messages_with_metadata():
    stash_count = 3
    calls = []

    def handler(msg, frm):
        nonlocal stash_count
        calls.append((msg, frm))
        if stash_count > 0:
            stash_count -= 1
            return STASH, "reason"
        else:
            return None, None

    bus = InternalBus()
    router = StashingRouter(10, buses=[bus])
    router.subscribe(SomeMessage, handler)

    msg_a = create_some_message()
    msg_b = create_some_message()
    bus.send(msg_a, 'A')
    bus.send(msg_b, 'B')
    assert router.stash_size() == 2
    assert calls == [(msg_a, 'A'), (msg_b, 'B')]

    router.process_all_stashed()
    assert router.stash_size() == 1
    assert calls == [(msg_a, 'A'), (msg_b, 'B'), (msg_a, 'A'), (msg_b, 'B')]

    router.process_all_stashed()
    assert router.stash_size() == 0
    assert calls == [(msg_a, 'A'), (msg_b, 'B'), (msg_a, 'A'), (msg_b, 'B'), (msg_a, 'A')]

    router.process_all_stashed()
    assert router.stash_size() == 0
    assert calls == [(msg_a, 'A'), (msg_b, 'B'), (msg_a, 'A'), (msg_b, 'B'), (msg_a, 'A')]


def test_stashing_router_can_stash_messages_with_different_reasons():
    calls = []

    def handler(message: SomeMessage):
        calls.append(message)
        if message.int_field % 2 == 0:
            return STASH + 0, "reason"
        else:
            return STASH + 1, "reason"

    bus = InternalBus()
    router = StashingRouter(10, buses=[bus])
    router.subscribe(SomeMessage, handler)

    messages = [create_some_message() for _ in range(10)]
    for msg in messages:
        bus.send(msg)
    assert router.stash_size() == len(messages)
    assert router.stash_size(STASH + 0) + router.stash_size(STASH + 1) == router.stash_size()

    calls.clear()
    router.process_all_stashed()
    assert router.stash_size() == len(messages)
    assert calls == sorted(messages, key=lambda m: m.int_field % 2)

    calls.clear()
    router.process_all_stashed(STASH + 0)
    assert router.stash_size() == len(messages)
    assert router.stash_size(STASH + 0) == len(calls)
    assert all(msg.int_field % 2 == 0 for msg in calls)
    assert all(msg in messages for msg in calls)

    calls.clear()
    router.process_all_stashed(STASH + 1)
    assert router.stash_size() == len(messages)
    assert router.stash_size(STASH + 1) == len(calls)
    assert all(msg.int_field % 2 != 0 for msg in calls)
    assert all(msg in messages for msg in calls)


def test_stashing_router_can_stash_and_sort_messages():
    calls = []

    def handler(message: SomeMessage):
        calls.append(message)
        return STASH, "reason"

    def sort_key(message: SomeMessage):
        return message.int_field

    bus = InternalBus()
    router = StashingRouter(10, buses=[bus])
    router.set_sorted_stasher(STASH, key=sort_key)
    router.subscribe(SomeMessage, handler)

    messages = [create_some_message() for _ in range(10)]
    for msg in messages:
        bus.send(msg)

    assert calls == messages

    calls.clear()
    router.process_all_stashed()
    assert calls == sorted(messages, key=sort_key)


def test_stashing_router_can_process_stashed_until_first_restash():
    calls = []

    def handler(msg):
        calls.append(msg)
        if len(calls) % 2 != 0:
            return STASH, "reason"
        else:
            return None, None

    bus = InternalBus()
    router = StashingRouter(10, buses=[bus])
    router.subscribe(SomeMessage, handler)

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
    assert router.stash_size() == 3
    assert calls == [msg_a, msg_b, msg_c, msg_d, msg_e]

    # Stash contains A, C, E, going to stop on C
    router.process_stashed_until_first_restash()
    assert router.stash_size() == 2
    assert calls == [msg_a, msg_b, msg_c, msg_d, msg_e, msg_a, msg_c]

    # Stash contains E, C, going to stop on C
    router.process_stashed_until_first_restash()
    assert router.stash_size() == 1
    assert calls == [msg_a, msg_b, msg_c, msg_d, msg_e, msg_a, msg_c, msg_e, msg_c]

    # Stash contains C, not going to stop
    router.process_stashed_until_first_restash()
    assert router.stash_size() == 0
    assert calls == [msg_a, msg_b, msg_c, msg_d, msg_e, msg_a, msg_c, msg_e, msg_c, msg_c]

    # Stash doesn't contain anything
    router.process_stashed_until_first_restash()
    assert router.stash_size() == 0
    assert calls == [msg_a, msg_b, msg_c, msg_d, msg_e, msg_a, msg_c, msg_e, msg_c, msg_c]
