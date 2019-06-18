from abc import ABC, abstractmethod
from asyncio import Queue, QueueFull, wait
from collections import deque
from inspect import isawaitable, iscoroutinefunction
from typing import Any, Type, Callable, Tuple, Optional, Dict


# TODO: After playing with concept a bit it feels like we don't actually need this
#  abstraction, and getting rid of it can make code more straightforward
class TxChannel(ABC):
    @abstractmethod
    async def put(self, msg: Any):
        pass

    @abstractmethod
    def put_nowait(self, msg: Any) -> bool:
        pass


# TODO: Rename to stream/observable?
class RxChannel(ABC):
    @abstractmethod
    # TODO: Optimization - adding subscriptions per type can reduce need for
    #  separate router and improve performance. However coupling implications
    #  need more analysis.
    def subscribe(self, handler: Callable):
        pass


# TODO: We don't have async handlers now - so we don't need this complexity
#  If we ever actually need async handlers then we might just as well create
#  separate AsyncRxChannel/AsyncStream
class _RxChannel(RxChannel):
    def __init__(self):
        self._sync_handlers = []
        self._async_handlers = []

    def subscribe(self, handler: Callable):
        if iscoroutinefunction(handler):
            self._async_handlers.append(handler)
        else:
            self._sync_handlers.append(handler)

    @property
    def has_async_handlers(self):
        return len(self._async_handlers) > 0

    def notify_sync(self, msg: Any):
        for handler in self._sync_handlers:
            handler(msg)

    async def notify(self, msg: Any):
        self.notify_sync(msg)
        if self.has_async_handlers:
            await wait([handler(msg) for handler in self._async_handlers])


# TODO: Having single DirectChannel implementing RxChannel interface would make
#  usage much simpler
def create_direct_channel() -> Tuple[TxChannel, RxChannel]:
    class Channel(TxChannel):
        def __init__(self, observable: _RxChannel):
            self._observable = observable

        async def put(self, msg: Any):
            await self._observable.notify(msg)

        def put_nowait(self, msg: Any):
            # TODO: What if observable has async handlers?
            self._observable.notify_sync(msg)
            return True

    router = _RxChannel()
    return Channel(router), router


# TODO: This was a PoC implementation to show how current queues could be implemented
#  using channel framework. This is actually not used and untested, probably remove it?
class QueuedChannelService:
    class Channel(TxChannel):
        def __init__(self, queue: deque):
            self._queue = queue

        async def put(self, msg: Any):
            self._queue.append(msg)

        def put_nowait(self, msg: Any):
            self._queue.append(msg)

    def __init__(self):
        self._queue = deque()
        self._inbox = self.Channel(self._queue)
        self._observable = _RxChannel()

    def inbox(self) -> TxChannel:
        return self._inbox

    def observable(self) -> RxChannel:
        return self._observable

    async def service(self, limit: Optional[int] = None) -> int:
        count = 0
        while len(self._queue) > 0 and (limit is None or count < limit):
            count += 1
            msg = self._queue.popleft()
            await self._observable.notify(msg)
        return count

    def service_sync(self, limit: Optional[int] = None) -> int:
        count = 0
        while len(self._queue) > 0 and (limit is None or count < limit):
            count += 1
            msg = self._queue.popleft()
            self._observable.notify_sync(msg)
        return count


# TODO: This was a PoC implementation to show how transparent port to AsyncIO could be
#  performed using channel framework. This is actually not used and untested, probably
#  remove it?
class AsyncioChannelService:
    class Channel(TxChannel):
        def __init__(self, queue: Queue):
            self._queue = queue

        async def put(self, msg: Any):
            await self._queue.put(msg)

        def put_nowait(self, msg: Any) -> bool:
            try:
                self._queue.put_nowait(msg)
                return True
            except QueueFull:
                return False

    def __init__(self):
        self._queue = Queue()
        self._inbox = self.Channel(self._queue)
        self._observable = _RxChannel()
        self._is_running = False

    def inbox(self) -> TxChannel:
        return self._inbox

    def router(self) -> RxChannel:
        return self._observable

    async def run(self):
        self._is_running = True
        while self._is_running:
            msg = await self._queue.get()
            await self._observable.notify(msg)

    def stop(self):
        self._is_running = False


# TODO: If we make default channel synchronous then we can just merge this
#  with Router. If we ever need async router we can just create a new implementation
#  relying on async channels and coroutines
class RouterBase:
    def __init__(self, strict: bool = False):
        self._strict = strict
        self._routes = {}  # type: Dict[Type, Callable]

    def add(self, msg_type: Type, handler: Callable):
        self._routes[msg_type] = handler

    def _process_sync(self, msg: Any):
        # This is done so that messages can include additional metadata
        # isinstance is not used here because it returns true for NamedTuple
        # as well.
        if type(msg) != tuple:
            msg = (msg,)
        handler = self._find_handler(msg[0])
        if handler is None:
            if self._strict:
                raise RuntimeError("unhandled msg: {}".format(msg))
            return
        return handler(*msg)

    def _find_handler(self, msg: Any) -> Optional[Callable]:
        for cls, handler in self._routes.items():
            if isinstance(msg, cls):
                return handler


class Router(RouterBase):
    def __init__(self, input: RxChannel, strict: bool = False):
        RouterBase.__init__(self, strict)
        input.subscribe(self._process_sync)

    def add(self, msg_type: Type, handler: Callable):
        if iscoroutinefunction(handler):
            raise ValueError('Router works only with synchronous handlers')
        RouterBase.add(self, msg_type, handler)


# TODO: We don't have async handlers now, remove it?
class AsyncRouter(RouterBase):
    def __init__(self, input: RxChannel, strict: bool = True):
        RouterBase.__init__(self, strict)
        input.subscribe(self._process)

    async def _process(self, msg: Any):
        result = self._process_sync(msg)
        return await result if isawaitable(result) else result
