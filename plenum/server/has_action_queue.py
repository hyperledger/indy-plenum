import time
from collections import deque
from typing import Callable

from plenum.common.log import getlogger

logger = getlogger()


class HasActionQueue:
    def __init__(self):
        self.actionQueue = deque()  # holds a deque of Callables; use functools.partial if the callable needs arguments
        self.aqStash = deque()
        self.aqNextCheck = float('inf')  # next time to check
        self.aid = 0  # action id

    def _schedule(self, action: Callable, seconds: int=0) -> None:
        """
        Schedule an action to be executed after `seconds` seconds.

        :param action: a callable to be scheduled
        :param seconds: the time in seconds after which the action must be executed
        """
        self.aid += 1
        if seconds > 0:
            nxt = time.perf_counter() + seconds
            if nxt < self.aqNextCheck:
                self.aqNextCheck = nxt
            logger.debug("{} scheduling action {} with id {} to run in {} "
                          "seconds".format(self, action, self.aid, seconds))
            self.aqStash.append((nxt, (action, self.aid)))
        else:
            logger.debug("{} scheduling action {} with id {} to run now".
                          format(self, action, self.aid))
            self.actionQueue.append((action, self.aid))
        return self.aid

    def _serviceActions(self) -> int:
        """
        Run all pending actions in the action queue.

        :return: number of actions executed.
        """
        if self.aqStash:
            tm = time.perf_counter()
            if tm > self.aqNextCheck:
                earliest = float('inf')
                for d in list(self.aqStash):
                    nxt, action = d
                    if tm > nxt:
                        self.actionQueue.appendleft(action)
                        self.aqStash.remove(d)
                    if nxt < earliest:
                        earliest = nxt
                self.aqNextCheck = earliest
        count = len(self.actionQueue)
        while self.actionQueue:
            action, aid = self.actionQueue.popleft()
            logger.debug("{} running action {} with id {}".
                         format(self, action, aid))
            action()
        return count
