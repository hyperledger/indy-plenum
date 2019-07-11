from abc import ABC, abstractmethod
from functools import partial
from typing import Callable, Any, Dict, Type, Optional, Iterable

from sortedcontainers import SortedListWithKey

from stp_core.common.log import getlogger

DISCARD = -1
PROCESS = 0
STASH = 1


class StashingQueue(ABC):
    @abstractmethod
    def __len__(self) -> int:
        pass

    @abstractmethod
    def push(self, item: Any) -> bool:
        """
        Try to add item to stash, returns True if successful, False otherwise
        """
        pass

    @abstractmethod
    def pop(self) -> Any:
        """
        Pop next item from queue, raise exception on failure
        """
        pass

    @abstractmethod
    def pop_all(self) -> Iterable:
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

    def push(self, item: Any) -> bool:
        if len(self._data) >= self._limit:
            return False
        self._data.append(item)
        return True

    def pop(self) -> Any:
        return self._data.pop(0)

    def pop_all(self) -> Iterable:
        data = self._data
        self._data = []
        return data


class SortedStash(StashingQueue):
    def __init__(self, limit: int, key: Callable):
        self._limit = limit
        self._key = lambda v: key(v[0])
        self._data = SortedListWithKey(key=self._key)

    def __len__(self):
        return len(self._data)

    def push(self, item: Any) -> bool:
        if len(self._data) >= self._limit:
            return False
        self._data.add(item)
        return True

    def pop(self) -> Any:
        return self._data.pop(0)

    def pop_all(self) -> Iterable:
        data = self._data
        self._data = SortedListWithKey(key=self._key)
        return data


class StashingRouter:
    Handler = Callable[..., Optional[int]]

    def __init__(self, limit: int):
        self._limit = limit
        self._logger = getlogger()
        self._handlers = {}  # type: Dict[Type, StashingRouter.Handler]
        self._queues = {}  # type: Dict[int, StashingQueue]

    def set_sorted_stasher(self, code: int, key: Callable):
        self._queues[code] = SortedStash(self._limit, key)

    def subscribe(self, message_type: Type, handler: Handler):
        # TODO: Discuss - we could throw custom logic error here, but is it really better than simple assert?
        assert message_type not in self._handlers
        self._handlers[message_type] = handler

    def subscribe_to(self, bus: Any):
        for message_type, handler in self._handlers.items():
            bus.subscribe(message_type, partial(self._process, handler))

    def process_stashed(self, code: Optional[int] = None, stop_on_stash: bool = None):
        """
        Try to process all stashed messages, re-stashing some of them if needed

        :param code: stash code, None if we need to unstash all
        :param stop_on_stash: whether processing should stop on first message that need to be re-stashed
        """
        if code is None:
            for code in sorted(self._queues.keys()):
                self.process_stashed(code, stop_on_stash)
            return

        queue = self._queues[code]
        if stop_on_stash:
            while queue:
                msg_tuple = queue.pop()
                if not self._resolve_and_process(*msg_tuple):
                    break
        else:
            data = queue.pop_all()
            for msg_tuple in data:
                self._resolve_and_process(*msg_tuple)

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
        code = handler(message, *args)

        # If handler returned either None or PROCESS we assume it successfully processed message
        # and no further action is needed
        if not code:
            return True

        if code == DISCARD:
            # TODO: Discuss - I know, this looks ugly, despite being generic. One option
            #  is to actually tie StashingRouter to ExternalBus and lose ability to subscribe
            #  to InternalBus (this can be viable solution since I don't expect using stashers with
            #  InternalBus). Another option is to create different impementations for external and
            #  internal buses with some common core. And yet another option is to just forfeit logging
            #  from stasher.
            self._logger.debug("Discarded message {} with metadata {}".format(message, args))
            return True

        self._stash(code, message, *args)
        return False

    def _resolve_and_process(self, message: Any, *args) -> bool:
        handler = self._handlers[type(message)]
        return self._process(handler, message, *args)

    def _stash(self, code: int, message: Any, *args):
        self._logger.debug("Stashing message {} with metadata {}".format(message, args))

        queue = self._queues.setdefault(code, UnsortedStash(self._limit))
        if not queue.push((message, *args)):
            # TODO: This is actually better be logged on info level with some throttling applied,
            #  however this cries for some generic easy to use solution, which we don't have yet.
            self._logger.debug("Cannot stash message {} with metadata {} - queue is full".format(message, args))
