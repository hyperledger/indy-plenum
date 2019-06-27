from typing import Any

from hypothesis import strategies as st

from plenum.test.simulation.sim_random import SimRandom


class HypothesisSimRandom(SimRandom):
    def __init__(self, data):
        self._data = data

    def integer(self, min_value: int, max_value: int) -> int:
        return self._data.draw(st.integers(min_value=min_value,
                                           max_value=max_value))

    def choice(self, *args) -> Any:
        return self._data.draw(st.sampled_from(args))
