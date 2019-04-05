from collections import deque, OrderedDict
from inspect import isawaitable
from typing import Callable, Any, NamedTuple, Union, Iterable
from typing import Tuple

from stp_core.common.log import getlogger

logger = getlogger()

Route = Tuple[Union[type, NamedTuple], Callable]


# TODO INDY-1983 revert changes, seems they are not necessary
class Router:
    """
    A simple router.

    Routes messages to functions based on their type.

    Constructor takes an iterable of tuples of
    (1) a class type and
    (2) a function that handles the message
    """

    def __init__(self, *routes: Route):
        """
        Create a new router with a list of routes

        :param routes: each route is a tuple of a type and a callable, so that
        the router knows which callable to invoke when presented with an object
         of a particular type.
        """
        self.routes = OrderedDict(routes)

    def add(self, route: Route):
        k, v = route
        self.routes[k] = v

    def extend(self, routes: Iterable[Route]):
        for r in routes:
            self.add(r)

    def remove(self, routes: Iterable[Route]):
        for k in routes:
            self.routes.pop(k, None)

    def getFunc(self, o: Any) -> Callable:
        """
        Get the next function from the list of routes that is capable of
        processing o's type.

        :param o: the object to process
        :return: the next function
        """
        for cls, func in self.routes.items():
            if isinstance(o, cls):
                return func
        logger.error("Unhandled msg {}, available handlers are:".format(o))
        for cls in self.routes.keys():
            logger.error("   {}".format(cls))
        raise RuntimeError("unhandled msg: {}".format(o))

    # noinspection PyCallingNonCallable
    def handleSync(self, msg: Any, key: Callable = None) -> Any:
        """
        Pass the message as an argument to the function defined in `routes`.

        :param msg: a message to route
        :param key (optional): callable to get route key. Defaults to message itself
        :return: the result of execution of the function corresponding to this message's type
        """
        return self.getFunc(msg if key is None else key(msg))(msg)

    async def handle(self, msg: Any, key: Callable = None) -> Any:
        """
        Handle both sync and async functions.

        :param msg: a message to route
        :param key (optional): callable to get route key. Defaults to message itself
        :return: the result of execution of the function corresponding to this message's type
        """
        res = self.handleSync(msg, key)
        if isawaitable(res):
            return await res
        else:
            return res

    async def handleAll(self, deq: deque, limit=None, key: Callable = None) -> int:
        """
        Handle all items in a deque. Can call asynchronous handlers.

        :param deq: a deque of items to be handled by this router
        :param limit (optional): the number of items in the deque to the handled
        :param key (optional): callable to get route key. Defaults to message itself
        :return: the number of items handled successfully
        """
        count = 0
        while deq and (not limit or count < limit):
            count += 1
            item = deq.popleft()
            await self.handle(item, key)
        return count

    def handleAllSync(self, deq: deque, limit=None, key: Callable = None) -> int:
        """
        Synchronously handle all items in a deque.

        :param deq: a deque of items to be handled by this router
        :param limit: the number of items in the deque to the handled
        :param key (optional): callable to get route key. Defaults to message itself
        :return: the number of items handled successfully
        """
        count = 0
        while deq and (not limit or count < limit):
            count += 1
            msg = deq.popleft()
            self.handleSync(msg, key)
        return count
