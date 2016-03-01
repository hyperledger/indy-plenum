from collections import deque, OrderedDict
from inspect import isawaitable
from typing import Callable, Any
from typing import Tuple

from plenum.common.util import error


class Router:
    """
    A simple router.

    Routes messages to functions based on their type.

    Constructor takes an iterable of tuples of
    (1) a class type and
    (2) a function that handles the message
    """

    def __init__(self, *routes: Tuple[type, Callable]):
        """
        Create a new router with a list of routes

        :param routes: each route is a tuple of a type and a callable, so that the router knows which
        callable to invoke when presented with an object of a particular type.
        """
        self.routes = OrderedDict(routes)

    def getFunc(self, o: Any) -> Callable:
        """
        Get the next function from the list of routes that is capable of processing o's type.

        :param o: the object to process
        :return: the next function
        """
        try:
            return next(
                func for cls, func in self.routes.items()
                if isinstance(o, cls))
        except StopIteration:
            raise RuntimeError("unhandled msg: {}".format(o))

    # noinspection PyCallingNonCallable
    def handleSync(self, msg: Any) -> Any:
        """
        Pass the message as an argument to the function defined in `routes`.
        If the msg is a tuple, pass the values as multiple arguments to the function.

        :param msg: tuple of object and callable
        """
        if isinstance(msg, tuple) and len(msg) == 2:
            return self.getFunc(msg[0])(*msg)
        else:
            return self.getFunc(msg)(msg)

    async def handle(self, msg: Any) -> Any:
        """
        Handle both sync and async functions.

        :param msg: a message
        :return: the result of execution of the function corresponding to this message's type
        """
        res = self.handleSync(msg)
        if isawaitable(res):
            return await res
        else:
            return res

    async def handleAll(self, deq: deque, limit=None) -> int:
        """
        Handle multiple messages passed as a deque.

        :param deq: a deque of messages to be handled by this router
        :param limit: the number of messages in the deque to the handled
        :return: the number of message handled successfully
        """
        count = 0
        while deq and (not limit or count < limit):
            count += 1
            msg = deq.popleft()
            await self.handle(msg)
        return count
