import string
from abc import ABC, abstractmethod
from random import Random
from typing import Any, Iterable, List, Optional


class SimRandom(ABC):
    @abstractmethod
    def integer(self, min_value: int, max_value: int) -> int:
        pass

    @abstractmethod
    def float(self, min_value: float, max_value: float) -> float:
        pass

    @abstractmethod
    def string(self, min_len: int, max_len: Optional[int] = None,
               alphabet: Optional[str] = string.ascii_letters + string.digits) -> str:
        pass

    @abstractmethod
    def choice(self, *args) -> Any:
        pass

    @abstractmethod
    def sample(self, population: List, num: int) -> List:
        pass

    @abstractmethod
    def shuffle(self, items: List) -> List:
        pass


class DefaultSimRandom(SimRandom):
    def __init__(self, seed=0):
        self._random = Random(seed)

    def integer(self, min_value: int, max_value: int) -> int:
        return self._random.randint(min_value, max_value)

    def float(self, min_value: float, max_value: float) -> float:
        return self._random.uniform(min_value, max_value)

    def string(self, min_len: int, max_len: Optional[int] = None,
               alphabet: Optional[str] = string.ascii_letters + string.digits) -> str:
        if max_len is None:
            max_len = min_len

        _len = self.integer(min_len, max_len)
        return ''.join(self.choice(*alphabet) for _ in range(_len))

    def choice(self, *args) -> Any:
        return self._random.choice(args)

    def sample(self, population: Iterable, num: int) -> List:
        return self._random.sample(population, num)

    def shuffle(self, items: List) -> List:
        result = items.copy()
        self._random.shuffle(result)
        return result
