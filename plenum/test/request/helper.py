import pytest

from typing import Iterable
from plenum.server.stateful import TransitionError

def check_transitions(stfl, states: Iterable, expected: Iterable):
    for st in states:
        if st in expected:
            stfl.tryState(st)
        else:
            with pytest.raises(TransitionError):
                stfl.tryState(st)

def check_statefuls(stfls, stfls_pass, check):
    for stfl in stfls:
        if stfl not in stfls_pass:
            with pytest.raises(TransitionError):
                check(stfl)
        else:
            check(stfl)
