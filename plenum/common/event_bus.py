from collections import defaultdict
from typing import Callable, Type, Dict, List


class EventBus:
    def __init__(self):
        self._handlers = defaultdict(list)  # type: Dict[Type, List[Callable]]

    def subscribe(self, message_type: Type, handler: Callable):
        self._handlers[message_type].append(handler)

    def send(self, *args):
        handlers = self._handlers[type(args[0])]
        for handler in handlers:
            handler(*args)
