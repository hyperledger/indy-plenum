from abc import abstractmethod, ABCMeta
from typing import Dict
from collections import Iterable

from stp_core.common.log import getlogger

logger = getlogger()


class TransitionError(Exception):
    """Exception raised for incorrect state transitions

    :param object: object of state transition
    :param state: new/desired RBFTRequest state (RBFTReqState)
    """
    def __init__(self, *args, **kwargs):
        self.stateful = kwargs.pop('stateful', None)
        self.state = kwargs.pop('state', None)
        super().__init__(*args, **kwargs)

    def __repr__(self):
        return (
            "stateful: {!r}, desired state: {!r}"
            .format(self.stateful, self.state)
        )


class Stateful:
    """
    Base class for states
    """
    def __init__(self,
                 initialState,
                 transitions: Dict,
                 name: str=None,
                 stateful_event_class=None):

        self.transitions = transitions
        self.states = [initialState]
        self.name = name
        self.stateful_event_class = stateful_event_class

    def __repr__(self):
        return "{}: states: {}".format(
            self.__class__.__name__ if self.name is None else self.name,
            self.states)

    def tryState(self, state):
        def trWrapper(trRule):
            def _defaultF():
                if isinstance(trRule, Iterable):
                    return self.state() in trRule
                else:
                    return self.state() == trRule
            return trRule if callable(trRule) else _defaultF

        if not trWrapper(self.transitions.get(state, []))():
            raise TransitionError(stateful=self, state=state)

        return None

    def setState(self, state, dry: bool=False, expectTrError=False):
        try:
            self.tryState(state)
        except TransitionError:
            if not expectTrError:
                raise
        else:
            if not dry:
                self.states.append(state)
                logger.trace("{!r} changed state from {!r} to {!r}"
                             .format(self, self.state(), state))

    def state(self):
        return self.states[-1]

    def wasState(self, state):
        return state in self.states

    def event(self, event, dry: bool=False):
        if self.stateful_event_class is None:
            # TODO use custom exception
            raise RuntimeError(
                "{} doesn't support any events processing, got object "
                "of type {}: {}"
                .format(self, self.stateful_event_class, type(event), event))
        elif not isinstance(event, self.stateful_event_class):
            raise TypeError(
                "{} expects {} for events but got object of type {}: {}"
                .format(self, self.stateful_event_class, type(event), event))

        logger.trace("{!r} processing new event {!r}".format(self, event))

        event.react(self, dry)


class StatefulEvent(metaclass=ABCMeta):
    def __repr__(self):
        return "{}".format(self.__class__.__name__)

    @abstractmethod
    def react(self, stateful: Stateful, dry: bool=False):
        pass
