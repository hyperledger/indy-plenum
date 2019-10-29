from abc import ABC, abstractmethod
from functools import partial
from typing import Callable, Any, Dict, Type, Optional, Iterable, Tuple, List

from sortedcontainers import SortedListWithKey

from common.exceptions import LogicError
from plenum.common.router import Router, Subscription
from stp_core.common.log import getlogger

DISCARD = -1
PROCESS = 0
STASH = 1


class StashingQueue(ABC):
    @abstractmethod
    def __len__(self) -> int:
        pass

    @abstractmethod
    def push(self, item, *args) -> bool:
        """
        Try to add item to stash, returns True if successful, False otherwise
        """
        pass

    @abstractmethod
    def pop(self) -> Tuple:
        """
        Pop next item from queue, raise exception on failure
        """
        pass

    @abstractmethod
    def pop_all(self) -> Iterable[Tuple]:
        """
        Remove all items from queue and return them
        """
        pass


class UnsortedStash(StashingQueue):
    def __init__(self, limit: int):
        self._limit = limit
        self._data = []

    def __len__(self):
        return len(self._data)

    def push(self, item, *args) -> bool:
        if len(self._data) >= self._limit:
            return False
        self._data.append((item, *args))
        return True

    def pop(self) -> Tuple:
        return self._data.pop(0)

    def pop_all(self) -> Iterable[Tuple]:
        data = self._data
        self._data = []
        return data

    def __iter__(self):
        return self._data.__iter__()


class SortedStash(StashingQueue):
    def __init__(self, limit: int, key: Callable):
        self._limit = limit
        self._key = lambda v: key(v[0])
        self._data = SortedListWithKey(key=self._key)

    def __len__(self):
        return len(self._data)

    def push(self, item, *args) -> bool:
        if len(self._data) >= self._limit:
            return False
        self._data.add((item, *args))
        return True

    def pop(self) -> Tuple:
        return self._data.pop(0)

    def pop_all(self) -> Iterable[Tuple]:
        data = self._data
        self._data = SortedListWithKey(key=self._key)
        return data


class StashingRouter(Router):
    Handler = Callable[..., Optional[Tuple[int, str]]]

    def __init__(self, limit: int, buses: List[Router], unstash_handler: Callable = None):
        super().__init__()
        self._limit = limit
        self._logger = getlogger()
        self._queues = {}  # type: Dict[int, StashingQueue]
        # TODO: This call has been added to saving the old message order in the list.
        # This is a replica's method that moves the message to the inBox, rather than
        # calling the handler immediately, as the default router does.
        self._unstash_handler = unstash_handler
        self._subscriptions = Subscription()
        self._buses = buses

    def set_sorted_stasher(self, code: int, key: Callable):
        self._queues[code] = SortedStash(self._limit, key)

    def subscribe(self, message_type: Type, handler: Handler) -> Router.SubscriptionID:
        # TODO: subscribe to one bus only
        for bus in self._buses:
            self._subscriptions.subscribe(bus,
                                          message_type,
                                          partial(self._process, handler))
        return super().subscribe(message_type, handler)

    def unsubscribe_from_all(self):
        self._subscriptions.unsubscribe_all()

    def process_all_stashed(self, code: Optional[int] = None):
        """
        Try to process all stashed messages, re-stashing some of them if needed

        :param code: stash code, None if we need to unstash all
        """
        if code is None:
            for code in sorted(self._queues.keys()):
                self.process_all_stashed(code)
            return

        queue = self._queues.get(code)
        if not queue:
            return

        data = queue.pop_all()
        self._logger.info("Unstash {} messages from stash with code {}".format(len(data), code))
        for msg_tuple in data:
            self._resolve_and_process(*msg_tuple)

    def process_stashed_until_first_restash(self, code: Optional[int] = None):
        """
        Try to process all stashed messages until handler indicates that some message
        needs to be stashed again (this can be especially useful with sorted stashes).

        :param code: stash code, None if we need to unstash all
        """
        if code is None:
            for code in sorted(self._queues.keys()):
                self.process_stashed_until_first_restash(code)
            return

        queue = self._queues.get(code)
        while queue:
            msg_tuple = queue.pop()
            if not self._resolve_and_process(*msg_tuple):
                break

    def stash_size(self, code: Optional[int] = None):
        if code is None:
            return sum(len(q) for q in self._queues.values())

        queue = self._queues.get(code)
        return len(queue) if queue else 0

    def _process(self, handler: Handler, message: Any, *args) -> bool:
        """
        Tries to process message using given handler. Returns True if message
        was stashed for reprocessing in future, False otherwise.
        """
        result = handler(message, *args)
        code, reason = result if result else (None, None)

        # If handler returned either None or PROCESS we assume it successfully processed message
        # and no further action is needed
        if not code:
            return True

        if code == DISCARD:
            self.discard(message, args, reason)
            return True

        self._stash(code, reason, message, *args)
        return False

    def _resolve_and_process(self, message: Any, *args) -> bool:
        handlers = self.handlers(type(message))
        if len(handlers) == 0:
            raise LogicError("Handler for message {} not found".format(message))
        return self._unstash(handlers[0], message, *args)

    def _unstash(self, handler: Handler, message: Any, *args) -> bool:
        if self._unstash_handler is None:
            return self._process(handler, message, *args)
        else:
            self._unstash_handler((message, *args))

    def _stash(self, code: int, reason: str, message: Any, *args):
        self._logger.trace("Stashing message {} with metadata {} "
                           "with the reason {}".format(message, args, reason))

        queue = self._queues.setdefault(code, UnsortedStash(self._limit))
        if not queue.push(message, *args):
            # TODO: This is actually better be logged on info level with some throttling applied,
            #  however this cries for some generic easy to use solution, which we don't have yet.
            self._logger.debug("Cannot stash message {} with metadata {} - queue is full".format(message, args))

    def discard(self, msg, args, reason):
        self._logger.trace("Discarded message {} with metadata {} "
                           "with the reason {}".format(msg, args, reason))
