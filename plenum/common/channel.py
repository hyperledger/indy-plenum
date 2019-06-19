from abc import ABC, abstractmethod
from asyncio import Queue, QueueFull, wait
from collections import deque
from inspect import isawaitable, iscoroutinefunction
from typing import Any, Type, Callable, Tuple, Optional, Dict


# TODO: DEPRECATE
#  After playing with concept a bit it feels like using EventBus
#  is more appropriate.


class TxChannel(ABC):
    @abstractmethod
    async def put(self, msg: Any):
        pass

    @abstractmethod
    def put_nowait(self, msg: Any) -> bool:
        pass


class RxChannel(ABC):
    @abstractmethod
    def subscribe(self, handler: Callable):
        pass


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


class AsyncRouter(RouterBase):
    def __init__(self, input: RxChannel, strict: bool = True):
        RouterBase.__init__(self, strict)
        input.subscribe(self._process)

    async def _process(self, msg: Any):
        result = self._process_sync(msg)
        return await result if isawaitable(result) else result
