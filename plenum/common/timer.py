from abc import ABC, abstractmethod
from bisect import bisect_right
from collections import defaultdict
from typing import Callable

import time


class TimerInterface(ABC):
    @abstractmethod
    def schedule(self, delay: int, callback: Callable):
        pass

    @abstractmethod
    def cancel(self, callback: Callable):
        pass


class Timer(TimerInterface):
    def __init__(self, get_current_time = time.perf_counter):
        self._get_current_time = get_current_time
        self._timestamps = []
        self._callbacks = []

    def service(self):
        while len(self._timestamps) and self._timestamps[0] <= self._get_current_time():
            callback = self._pop_callback()
            callback()

    def schedule(self, delay: float, callback: Callable):
        timestamp = self._get_current_time() + delay
        i = bisect_right(self._timestamps, timestamp)
        self._timestamps.insert(i, timestamp)
        self._callbacks.insert(i, callback)

    def cancel(self, callback: Callable, cancel_all: bool = False):
        if cancel_all:
            self._cancel_all(callback)
        else:
            self._cancel(callback)

    def _pop_callback(self, index: int = 0) -> Callable:
        del self._timestamps[index]
        return self._callbacks.pop(index)

    def _cancel_all(self, callback: Callable):
        indexes = [i for i, cb in enumerate(self._callbacks) if cb == callback]
        for i in reversed(indexes):
            self._pop_callback(i)

    def _cancel(self, callback: Callable):
        for i, cb in enumerate(self._callbacks):
            if cb == callback:
                self._pop_callback(i)
                return
