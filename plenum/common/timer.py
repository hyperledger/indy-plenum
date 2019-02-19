from abc import ABC, abstractmethod
from bisect import bisect_right
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
    def __init__(self, get_current_time=time.perf_counter):
        self._get_current_time = get_current_time
        self._timestamps = []
        self._callbacks = []

    def queue_size(self):
        return len(self._callbacks)

    def service(self) -> int:
        count = 0
        while len(self._timestamps) and self._timestamps[0] <= self._get_current_time():
            callback = self._pop_callback()
            callback()
            count += 1
        return count

    def schedule(self, delay: float, callback: Callable):
        timestamp = self._get_current_time() + delay
        i = bisect_right(self._timestamps, timestamp)
        self._timestamps.insert(i, timestamp)
        self._callbacks.insert(i, callback)

    def cancel(self, callback: Callable):
        indexes = [i for i, cb in enumerate(self._callbacks) if cb == callback]
        for i in reversed(indexes):
            self._pop_callback(i)

    def _pop_callback(self, index: int = 0) -> Callable:
        del self._timestamps[index]
        return self._callbacks.pop(index)


class RepeatingTimer:
    def __init__(self, timer: TimerInterface, interval: int, callback: Callable, active: bool = True):
        self._timer = timer
        self._interval = interval
        self._callback = callback
        self._active = False
        if active:
            self.start()

    def start(self):
        if self._active:
            return
        self._active = True
        self._timer.schedule(self._interval, self._repeatable_callback)

    def stop(self):
        if not self._active:
            return
        self._active = False
        self._timer.cancel(self._repeatable_callback)

    def _repeatable_callback(self):
        if not self._active:
            return
        self._callback()
        self._timer.schedule(self._interval, self._repeatable_callback)
