from collections import deque, OrderedDict
from inspect import isawaitable
from typing import Callable, Any, NamedTuple, Union, Iterable
from typing import Tuple

Route = Tuple[Union[type, NamedTuple], Callable]


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
        # If a plain python tuple and not a named tuple, a better alternative
        # would be to create a named entity with the 3 characteristics below
        # TODO: non-obvious tuple, re-factor!
        if isinstance(msg, tuple) and len(
                msg) == 2 and not hasattr(msg, '_field_types'):
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
        Handle all items in a deque. Can call asynchronous handlers.

        :param deq: a deque of items to be handled by this router
        :param limit: the number of items in the deque to the handled
        :return: the number of items handled successfully
        """
        count = 0
        while deq and (not limit or count < limit):
            count += 1
            item = deq.popleft()
            await self.handle(item)
        return count

    def handleAllSync(self, deq: deque, limit=None) -> int:
        """
        Synchronously handle all items in a deque.

        :param deq: a deque of items to be handled by this router
        :param limit: the number of items in the deque to the handled
        :return: the number of items handled successfully
        """
        count = 0
        while deq and (not limit or count < limit):
            count += 1
            msg = deq.popleft()
            self.handleSync(msg)
        return count
