from collections import Iterable
from typing import Dict

from stp_core.common.log import getlogger

logger = getlogger()


class TransitionError(Exception):
    """Exception raised for incorrect state transitions

    :param object: object of state transition
    :param state: new/desired RBFTRequest state (RBFTReqState)
    """
    def __init__(self, *args, **kwargs):
        self.stateful = kwargs.pop('stateful')
        self.state = kwargs.pop('state')
        super().__init__(*args, **kwargs)

    def __str__(self):
        return (
            "stateful: {}, desired state: {}"
            .format(repr(self.stateful), repr(self.state))
        )


class Stateful:
    """
    Base class for states
    """
    def __init__(self, initialState, transitions: Dict):
        self.transitions = transitions
        self.states = [initialState]

    def __repr__(self):
        return "{}: states: {}".format(
            self.__class__.__name__,
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

    def setState(self, state, expectTrError=False):
        try:
            self.tryState(state)
        except TransitionError:
            if not expectTrError:
                raise
        else:
            self.states.append(state)

    def state(self):
        return self.states[-1]

    def wasState(self, state):
        return state in self.states
