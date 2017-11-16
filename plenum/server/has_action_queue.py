import time
from collections import deque
from functools import wraps
from typing import Callable

from stp_core.common.log import getlogger
from stp_core.common.util import get_func_name

logger = getlogger()


class HasActionQueue:
    def __init__(self):
        # holds a deque of Callables; use functools.partial if the callable
        # needs arguments
        self.actionQueue = deque()
        self.aqStash = deque()
        self.aqNextCheck = float('inf')  # next time to check
        self.aid = 0  # action id
        self.repeatingActions = set()
        self.scheduled = dict()

    def _schedule(self, action: Callable, seconds: int=0) -> int:
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
            logger.trace("{} scheduling action {} with id {} to run in {} "
                         "seconds".format(self, get_func_name(action),
                                          self.aid, seconds))
            self.aqStash.append((nxt, (action, self.aid)))
        else:
            logger.trace("{} scheduling action {} with id {} to run now".
                         format(self, get_func_name(action), self.aid))
            self.actionQueue.append((action, self.aid))

        if action not in self.scheduled:
            self.scheduled[action] = []
        self.scheduled[action].append(self.aid)

        return self.aid

    def _cancel(self, action: Callable = None, aid: int = None):
        """
        Cancel scheduled events

        :param action:  (optional) scheduled action. If specified, all
                scheduled events for the action are cancelled.
        :param aid:     (options) scheduled event id. If specified,
                scheduled event with the aid is cancelled.
        """
        if action is not None:
            if action in self.scheduled:
                logger.trace("{} cancelling all events for action {}, ids: {}"
                             "".format(self, action, self.scheduled[action]))
                self.scheduled[action].clear()
        elif aid is not None:
            for action, aids in self.scheduled.items():
                try:
                    aids.remove(aid)
                except ValueError:
                    pass
                else:
                    logger.trace("{} cancelled action {} with id {}".format(self, action, aid))
                    break

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
            assert action in self.scheduled
            if aid in self.scheduled[action]:
                self.scheduled[action].remove(aid)
                logger.trace("{} running action {} with id {}".
                             format(self, get_func_name(action), aid))
                action()
            else:
                logger.trace("{} not running cancelled action {} with id {}".
                             format(self, get_func_name(action), aid))
        return count

    def startRepeating(self, action: Callable, seconds: int):
        @wraps(action)
        def wrapper():
            if action in self.repeatingActions:
                action()
                self._schedule(wrapper, seconds)

        if action not in self.repeatingActions:
            logger.debug('{} will be repeating every {} seconds'.
                         format(get_func_name(action), seconds))
            self.repeatingActions.add(action)
            self._schedule(wrapper, seconds)
        else:
            logger.debug('{} is already repeating'.format(
                get_func_name(action)))

    def stopRepeating(self, action: Callable, strict=True):
        try:
            self.repeatingActions.remove(action)
            logger.debug('{} will not be repeating'.format(
                get_func_name(action)))
        except KeyError:
            msg = '{} not found in repeating actions'.format(
                get_func_name(action))
            if strict:
                raise KeyError(msg)
            else:
                logger.debug(msg)
