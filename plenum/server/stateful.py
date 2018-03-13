from abc import abstractmethod, ABCMeta
from typing import Dict
import inspect
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


class StatefulEvent(metaclass=ABCMeta):
    def __repr__(self):
        return "{}".format(self.__class__.__name__)


class StatefulMeta(type):

    def __new__(cls, name, bases, attrs, **kwargs):
        EV_METHOD_PREFIX = 'on_'

        def _on(self, ev, dry: bool=False):
            raise NotImplementedError("{}: method '_on'".format(self))

        def on(self, ev, dry: bool=False):
            if not self.supported_events:
                raise RuntimeError(
                    "{} doesn't support any events, got object of type {}: {}"
                    .format(self, type(ev), ev))
            elif type(ev) not in self.supported_events:
                raise TypeError(
                    "{} expects one of {} events but got object of type {}: {}"
                    .format(self, self.supported_events, type(ev), ev))

            logger.trace("{!r} processing new event {!r}, dry: {}".format(self, ev, dry))
            self._on(ev, dry)

        def on_ev_wrapper(ev_cls):
            def on_ev(self, *args, **kwargs):
                dry = kwargs.pop('dry', False)
                ev = ev_cls(*args, **kwargs)
                return self.on(ev, dry=dry)
            return on_ev

        result = type.__new__(cls, name, bases, attrs, **kwargs)

        _supported_events = set()
        for k, v in attrs.items():
            if inspect.isclass(v) and issubclass(v, StatefulEvent):
                ev_method_name = "{}{}".format(EV_METHOD_PREFIX, k.lower())
                if not hasattr(result, ev_method_name):
                    setattr(result, ev_method_name, on_ev_wrapper(v))
                    _supported_events.add(v)

        if hasattr(result, 'supported_events'):
            _supported_events |= set(result.supported_events)
        setattr(result, "supported_events", tuple(_supported_events))

        if not hasattr(result, '_on'):
            setattr(result, "_on", _on)
        if not hasattr(result, 'on'):
            setattr(result, "on", on)

        return result


class Stateful(metaclass=StatefulMeta):
    """
    Base class for states
    """
    def __init__(self,
                 initialState,
                 transitions: Dict,
                 name: str=None):

        self.transitions = transitions
        self.states = [initialState]
        self.name = name

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

    def setState(self, state, dry: bool=False):
        self.tryState(state)
        if not dry:
            self.states.append(state)
            logger.trace("{!r} changed state from {!r} to {!r}"
                         .format(self, self.state(), state))

    def state(self):
        return self.states[-1]

    def wasState(self, state):
        return state in self.states
