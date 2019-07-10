from collections import defaultdict
from functools import partial
from typing import Callable, Any, Dict, List, Type, Optional, Tuple

DISCARD = -1
PROCESS = 0
STASH = 1


class StashingRouter:
    Handler = Callable[..., Optional[int]]

    def __init__(self):
        self._handlers = {}  # type: Dict[Type, StashingRouter.Handler]
        self._queues = defaultdict(list)  # type: Dict[int, List[Tuple]]

    def subscribe(self, message_type: Type, handler: Handler):
        # TODO: Check double subscription?
        self._handlers[message_type] = handler

    def subscribe_to(self, bus: Any):
        for message_type, handler in self._handlers.items():
            bus.subscribe(message_type, partial(self._process, handler))

    def unstash(self, code: Optional[int] = None):
        if code is None:
            for c in list(self._queues.keys()):
                self._unstash(c)
        else:
            self._unstash(c)

    def _process(self, handler: Handler, message: Any, *args):
        result = handler(message, *args)

        # If handler returned either None or PROCESS we assume it successfully processed message
        # and no further action is needed
        if not result:
            return

        if result == DISCARD:
            # TODO: Log discard?
            return

        # TODO: Log stash?
        self._queues[result].append((message, *args))

    def _unstash(self, reason: int):
        queue = self._queues[reason]
        self._queues[reason] = []
        for msg_tuple in queue:
            msg_type = type(msg_tuple[0])
            handler = self._handlers[msg_type]
            self._process(handler, *msg_tuple)
