import pytest

from typing import Iterable
from plenum.server.stateful import TransitionError

def check_transitions(stateful, states: Iterable, expected: Iterable):
    for st in states:
        if st in expected:
            stateful.tryState(st)
        else:
            with pytest.raises(TransitionError):
                stateful.tryState(st)
