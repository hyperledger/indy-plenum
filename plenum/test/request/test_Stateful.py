import pytest

from plenum.server.stateful import (
    TransitionError,
    Stateful,
    StatefulEvent
)

def testInitialState():
    Stateful(1, {}).state() == 1

def testNoTransitionRule():
    with pytest.raises(TransitionError):
        Stateful(1, {}).tryState(2)

def testTransitionError():
    stateful = Stateful(1, {})
    with pytest.raises(TransitionError) as excinfo:
        stateful.tryState(2)
    assert excinfo.value.stateful is stateful
    assert excinfo.value.state == 2

def testNonIterableTransitionRule():
    Stateful(1, {2: 1}).tryState(2)

def testIterableTransitionRule():
    Stateful(1, {2: (1,)}).tryState(2)

def testCallableTransitionRule():
    Stateful(1, {2: lambda: True}).tryState(2)
    with pytest.raises(TransitionError):
        assert Stateful(1, {2: lambda: False}).tryState(2)

def testState():
    stateful = Stateful(1, {2: lambda: True})
    stateful.setState(2)
    assert stateful.state() == 2

    # do not raise TransitionError
    stateful.setState(3, expectTrError=True)
    assert stateful.state() == 2

def testStateHistory():
    stateful = Stateful(1, {2: 1, 4: 2})
    assert stateful.wasState(1)
    assert not stateful.wasState(2)
    stateful.setState(2)
    assert stateful.wasState(1)
    assert stateful.wasState(2)
    stateful.setState(4)
    assert stateful.wasState(2)

class BaseEvent(StatefulEvent):
    pass

class EventOk(BaseEvent):
    def react(self, stateful):
        pass

class EventWrong:
    pass

def testNoEventsExpected():
    with pytest.raises(RuntimeError) as excinfo:
        Stateful(1, {}).event(EventOk())
    assert "doesn't support any events processing" in str(excinfo.value)

def testNotSupportedEvent():
    with pytest.raises(TypeError) as excinfo:
        Stateful(1, {}, stateful_event_class=BaseEvent).event(EventWrong())
    assert ("expects {} for events but got object of type {}"
            .format(BaseEvent, EventWrong) in str(excinfo.value))

def testSupportedEvent():
    Stateful(1, {}, stateful_event_class=BaseEvent).event(EventOk())
