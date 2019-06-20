
from hypothesis import strategies as st

from plenum.test.simulation.sim_random import SimRandom


class HypothesisSimRandom(SimRandom):
    def __init__(self, data):
        self._data = data

    def integer(self, min: int, max: int) -> int:
        return self._data.draw(st.integer(min=min, max=max))

    def choice(self, *args) -> Any:
        return self._data.draw(st.sampled_from(args))
