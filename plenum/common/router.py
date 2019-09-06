from collections import defaultdict
from typing import Type, Callable, Any, Tuple, List, Dict


class Router:
    """
    Direct router implementation.
    Note:
    """
    SubscriptionID = Tuple[Type, Callable]

    def __init__(self):
        self._handlers = defaultdict(list)  # type: Dict[Type, List[Callable]]

    @property
    def message_types(self) -> List[Type]:
        """
        Returns list of handled message types
        """
        return [message_type
                for message_type, handlers in self._handlers.items()
                if len(handlers) > 0]

    def handlers(self, message_type: Type) -> List[Callable]:
        """
        Returns list of handlers for specific message_types
        """
        return self._handlers.get(message_type, [])

    def subscribe(self, message_type: Type, handler: Callable) -> SubscriptionID:
        """
        Subscribes to specific message type with callable, returns ID that can be used to unsubscribe
        """
        self._handlers[message_type].append(handler)
        return message_type, handler

    def unsubscribe(self, sid: SubscriptionID):
        """
        Unsubscribe some callable from some message using ID previously returned from subscribe
        """
        self._handlers[sid[0]].remove(sid[1])

    def _route(self, message: Any, *args) -> List:
        """
        Call handlers subscribed to message, return list of results of those calls.
        This is a protected method which is intended to be called from inheriting classes.
        """
        handlers = self._handlers[type(message)]
        return [handler(message, *args) for handler in handlers]


class Subscription:
    def __init__(self):
        self._subscriptions = []  # type: List[Tuple[Router, Router.SubscriptionID]]

    def subscribe(self, router: Router, message_type: Type, handler: Callable):
        """
        Subscribe handler to specific message on specific router
        """
        sid = router.subscribe(message_type, handler)
        self._subscriptions.append((router, sid))

    def unsubscribe_all(self):
        """
        Unsubscribe from all messages from all routers previously subscribed using this instance
        """
        for router, sid in self._subscriptions:
            router.unsubscribe(sid)
        self._subscriptions.clear()
