import pytest

from plenum.server.stateful import (
    TransitionError,
    StatefulMeta,
    Stateful,
    StatefulEvent
)


class StEvTest(StatefulEvent):
    def __init__(self, value):
        self.value = value

    def __eq__(self, other):
        return (isinstance(other, self.__class__)
                    and self.__dict__ == other.__dict__)

class StEvTest2(StatefulEvent):
    pass


class StEvTest3(StatefulEvent):
    pass


class StatefulNoEvent(metaclass=StatefulMeta):
    pass


class StatefulBase(metaclass=StatefulMeta):
    event1 = StEvTest


class StatefulChild(StatefulBase):
    Event2 = StEvTest
    EveNt_3 = StEvTest2

    def __init__(self):
        self.last_event = None

    def _on(self, ev, dry=False):
        self.last_event = (ev, dry)


def test_meta_api():
    assert set(getattr(StatefulBase, 'supported_events')) == set((StEvTest,))
    assert getattr(StatefulBase, 'event1')
    assert getattr(StatefulBase, 'on_event1')

    assert set(getattr(StatefulChild, 'supported_events')) == set((StEvTest, StEvTest2))
    assert getattr(StatefulChild, 'event1')
    assert getattr(StatefulChild, 'on_event1')
    assert getattr(StatefulChild, 'Event2')
    assert getattr(StatefulChild, 'on_event2')
    assert getattr(StatefulChild, 'EveNt_3')
    assert getattr(StatefulChild, 'on_event_3')


def test_meta_no_event():
    with pytest.raises(RuntimeError) as excinfo:
        StatefulNoEvent().on(StEvTest2())
    assert "doesn't support any events" in str(excinfo.value)

def test_meta_not_ready():
    with pytest.raises(NotImplementedError) as excinfo:
        StatefulBase().on_event1(5)
    assert "method '_on'" in str(excinfo.value)

def test_meta_ready():
    st_test = StatefulChild()
    assert st_test.last_event is None

    st_test.on_event1(1)
    assert st_test.last_event is not None
    assert type(st_test.last_event[0]) is StEvTest
    st_test.last_event[1] == False

    st_test.on_event_3()
    assert type(st_test.last_event[0]) is StEvTest2
    st_test.last_event[1] == False

    st_test.on_event2(2, dry=True)
    assert type(st_test.last_event[0]) is StEvTest
    st_test.last_event[1] == True

def test_meta_api_alternative():
    st_test = StatefulChild()
    test_event = StEvTest(2)

    st_test.on_event2(2, dry=True)
    assert st_test.last_event[0] is not test_event
    assert st_test.last_event[0] == test_event

    st_test.on(test_event, dry=True)
    assert st_test.last_event[0] is test_event
    assert st_test.last_event[0] == test_event

def test_meta_unkown_event():
    with pytest.raises(TypeError) as excinfo:
        StatefulChild().on(StEvTest3())
    assert ("expects one of {} events but got object of type {}"
            .format(StatefulChild.supported_events, StEvTest3))

def test_initial_state():
    Stateful(1, {}).state() == 1

def test_no_transition_rule():
    with pytest.raises(TransitionError):
        Stateful(1, {}).tryState(2)

def test_transition_error():
    stateful = Stateful(1, {})
    with pytest.raises(TransitionError) as excinfo:
        stateful.tryState(2)
    assert excinfo.value.stateful is stateful
    assert excinfo.value.state == 2

def test_non_iterable_transition_rule():
    Stateful(1, {2: 1}).tryState(2)

def test_iterable_transition_rule():
    Stateful(1, {2: (1,)}).tryState(2)

def test_callable_transition_rule():
    Stateful(1, {2: lambda: True}).tryState(2)
    with pytest.raises(TransitionError):
        assert Stateful(1, {2: lambda: False}).tryState(2)

def test_set_state_dry():
    stateful = Stateful(1, {2: lambda: True})
    stateful.setState(2, dry=True)
    assert stateful.state() == 1

def test_state():
    stateful = Stateful(1, {2: lambda: True})
    stateful.setState(2)
    assert stateful.state() == 2

def test_state_history():
    stateful = Stateful(1, {2: 1, 4: 2})
    assert stateful.wasState(1)
    assert not stateful.wasState(2)
    stateful.setState(2)
    assert stateful.wasState(1)
    assert stateful.wasState(2)
    stateful.setState(4)
    assert stateful.wasState(2)
