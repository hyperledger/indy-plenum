import asyncio
import inspect
import time
from asyncio.coroutines import CoroWrapper
from typing import List

from zeno.common.startable import Status
from zeno.common.util import getlogger

logger = getlogger()


class Nextable:
    """
    An interface that defines the methods next, start and get_status
    """

    async def next(self, limit) -> int:
        """
        Actions to be performed each time the Nextable object gets a chunk of CPU time.

        :param limit: the number of messages to be processed.
        """
        raise NotImplementedError("subclass {} should implement this method"
                                  .format(self))

    def start(self):
        """
        Actions to be performed when the Nextable is starting up.
        """
        raise NotImplementedError("subclass {} should implement this method"
                                  .format(self))

    def stop(self):
        """
        Actions to be performed when the Nextable is starting up.
        """
        raise NotImplementedError("subclass {} should implement this method"
                                  .format(self))

    def get_status(self) -> Status:
        """
        Get the current status of this Nextable
        """
        raise NotImplementedError("subclass {} should implement this method"
                                  .format(self))


class Looper:
    """
    A helper class for asyncio's event_loop
    """

    def __init__(self,
                 nextables: List[Nextable]=None,
                 loop=None,
                 debug=True,
                 autoStart=True):
        """
        Initialize looper with an event loop.

        :param nextables: a list of nextables that this event loop will execute
        :param loop: the event loop to use
        :param debug: set_debug on event loop will be set to this value
        :param autoStart: start immediately?
        """
        self.nextables = list(nextables) if nextables is not None else []  # type: List[Nextable]

        if loop:
            self.loop = loop
        else:
            try:
                l = asyncio.get_event_loop()
                if l.is_closed():
                    raise RuntimeError("event loop was closed")
            except Exception as ex:
                logger.warning("Looper could not get default event loop; "
                               "creating a new one: {}".format(ex))
                l = asyncio.new_event_loop()
                asyncio.set_event_loop(l)
            self.loop = l

        self.runFut = self.loop.create_task(self.runForever())  # type: Task
        self.running = True  # type: bool
        self.inContext = False  # type: bool
        self.loop.set_debug(debug)
        self.autoStart = autoStart  # type: bool
        if self.autoStart:
            self.startall()

    async def runOnce(self):
        """
        Call `next` once for each Nextable in this Looper

        :return: the sum of the number of events executed successfully
        """
        limit = None
        s = 0
        for n in self.nextables:
            s += await n.next(limit)
        return s

    def add(self, nextable: Nextable) -> None:
        """
        Add one Nextable object to this Looper's list of Nextables

        :param nextable: the Nextable object to add
        """
        self.nextables.append(nextable)
        if self.autoStart:
            nextable.start()

    def removeNextable(self, nextable: Nextable) -> None:
        """
        Remove the specified Nextable object from this Looper's list of Nextables

        :param nextable: the Nextable to remove
        """
        self.nextables.remove(nextable)

    async def runOnceNicely(self):
        """
        Execute `runOnce` with a small tolerance of 0.01 seconds so that the Nextables
        can complete their other asynchronous tasks not running on the event-loop.
        """
        start = time.perf_counter()
        msgsProcessed = await self.runOnce()
        if msgsProcessed == 0:
            await asyncio.sleep(0.01)  # if no let other stuff run
        dur = time.perf_counter() - start
        if dur >= 0.5:
            logger.warning("it took {:.3f} seconds to run once nicely".
                           format(dur), extra={"cli": False})

    def runFor(self, timeout):
        self.run(asyncio.sleep(timeout))

    async def runForever(self):
        """
        Keep calling `runOnceNicely` in an infinite loop.
        """
        while self.running:
            await self.runOnceNicely()

    def run(self, *coros: CoroWrapper):
        """
        Runs an arbitrary list of coroutines in order and then quits the loop,
        if not running as a context manager.
        """
        if not self.running:
            raise RuntimeError("not running!")

        async def wrapper():
            results = []
            for coro in coros:
                try:
                    if inspect.isawaitable(coro):
                        results.append(await coro)
                    elif inspect.isfunction(coro):
                        res = coro()
                        if inspect.isawaitable(res):
                            results.append(await res)
                        else:
                            results.append(res)
                    else:
                        raise RuntimeError("don't know how to run {}".format(coro))
                except Exception as ex:
                    logger.error("Error while running coroutine {}: {}"
                                 .format(coro.__name__, ex.__repr__()))
                    raise ex
            if len(results) == 1:
                return results[0]
            return results
        return self.loop.run_until_complete(wrapper())

    async def shutdown(self):
        """
        Shut down this Looper.
        """
        logger.info("Looper shutting down now...",
                    extra={"cli": False})
        self.running = False
        start = time.perf_counter()
        await self.runFut
        self.stopall()
        logger.info("Looper shut down in {:.3f} seconds.".
                    format(time.perf_counter() - start),
                    extra={"cli": False})

    def __enter__(self):
        self.inContext = True
        return self

    def shutdownSync(self):
        self.loop.run_until_complete(self.shutdown())

    # noinspection PyUnusedLocal
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdownSync()

    async def __aenter__(self):
        return self

    # noinspection PyUnusedLocal
    async def __aexit__(self, exc_type, exc, tb):
        await self.shutdown()

    def startall(self):
        """
        Start all the Nextables in this Looper's `nextables`
        """
        for n in self.nextables:
            n.start()

    def stopall(self):
        for n in self.nextables:
            n.stop()
