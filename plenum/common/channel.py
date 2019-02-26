from abc import ABC, abstractmethod
from asyncio import Queue, QueueFull
from collections import deque
from typing import Any, Type, Callable, Tuple

from plenum.server.router import Router


class TxChannel(ABC):
    @abstractmethod
    async def put(self, msg: Any):
        pass

    @abstractmethod
    def put_nowait(self, msg: Any) -> bool:
        pass


class RxChannel(ABC):
    @abstractmethod
    def set_handler(self, msg_type: Type, handler: Callable):
        pass


class _RouterRxChannel(Router, RxChannel):
    def set_handler(self, msg_type: Type, handler: Callable):
        self.add((msg_type, handler))


def create_direct_channel() -> Tuple[TxChannel, RxChannel]:
    class Channel(TxChannel):
        def __init__(self, router: Router):
            self._router = router

        async def put(self, msg: Any):
            await self._router.handle(msg)

        def put_nowait(self, msg: Any):
            self._router.handleSync(msg)
            return True

    router = _RouterRxChannel()
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
        self._router = _RouterRxChannel()

    def inbox(self) -> TxChannel:
        return self._inbox

    def router(self) -> RxChannel:
        return self._router

    async def service(self) -> int:
        return await self._router.handleAll(self._queue)

    def service_sync(self) -> int:
        return self._router.handleAllSync(self._queue)


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
        self._router = _RouterRxChannel()
        self._is_running = False

    def inbox(self) -> TxChannel:
        return self._inbox

    def router(self) -> RxChannel:
        return self._router

    async def run(self):
        self._is_running = True
        while self._is_running:
            msg = await self._queue.get()
            await self._router.handle(msg)

    def stop(self):
        self._is_running = False
