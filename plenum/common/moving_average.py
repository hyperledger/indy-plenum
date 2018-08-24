import math
from abc import ABC, abstractmethod


class MovingAverage(ABC):
    @abstractmethod
    def update(self, value: float):
        pass

    @abstractmethod
    def reset(self, value: float):
        pass

    @property
    @abstractmethod
    def value(self) -> float:
        return self._value


class ExponentialMovingAverage(MovingAverage):
    def __init__(self, alpha: float, start: float = 0.0):
        self._value = start
        self._alpha = alpha

    def __eq__(self, other):
        if not isinstance(other, ExponentialMovingAverage):
            return False
        if self._alpha != other._alpha:
            return False
        return self._value == other._value

    def update(self, value: float):
        self._value = value * self._alpha + self._value * (1 - self._alpha)

    def reset(self, value: float):
        self._value = value

    @property
    def value(self) -> float:
        return self._value

    @staticmethod
    def halfway_alpha(steps):
        return -math.log(0.5) / steps
