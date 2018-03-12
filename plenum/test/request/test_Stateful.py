import pytest

from plenum.server.stateful import (
    TransitionError,
    StatefulMeta,
    Stateful,
    StatefulEvent
)


class StEvTest(StatefulEvent):
    pass

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

def testMetaNoEvent():
    with pytest.raises(RuntimeError) as excinfo:
        StatefulNoEvent().on(StEvTest)
    assert "doesn't support any events" in str(excinfo.value)


def testMetaNotReady():
    assert set(getattr(StatefulBase, 'supported_events')) == set((StEvTest,))
    assert getattr(StatefulBase, 'on_event1')

    with pytest.raises(NotImplementedError) as excinfo:
        StatefulBase().on_event1()
    assert "method '_on'" in str(excinfo.value)

def testMetaReady():
    assert set(getattr(StatefulChild, 'supported_events')) == set((StEvTest, StEvTest2))
    assert getattr(StatefulChild, 'on_event1')
    assert getattr(StatefulChild, 'on_event2')
    assert getattr(StatefulChild, 'on_event_3')

    stTest = StatefulChild()
    assert stTest.last_event is None

    stTest.on_event1()
    assert stTest.last_event is not None
    assert type(stTest.last_event[0]) is StEvTest
    stTest.last_event[1] == False

    stTest.on_event_3()
    assert type(stTest.last_event[0]) is StEvTest2
    stTest.last_event[1] == False

    stTest.on_event2(dry=True)
    assert type(stTest.last_event[0]) is StEvTest
    stTest.last_event[1] == True

def testMetaUnkownEvent():
    with pytest.raises(TypeError) as excinfo:
        StatefulChild().on(StEvTest3())
    assert ("expects one of {} events but got object of type {}"
            .format(StatefulChild.supported_events, StEvTest3))


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

def testSetStateDry():
    stateful = Stateful(1, {2: lambda: True})
    stateful.setState(2, dry=True)
    assert stateful.state() == 1

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
