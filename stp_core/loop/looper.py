import os
import asyncio
import inspect
import signal
import sys
import time
from asyncio.coroutines import CoroWrapper
from typing import List, Optional

# import uvloop
from stp_core.common.log import getlogger
from stp_core.common.util import lxor
from stp_core.loop.exceptions import ProdableAlreadyAdded
from stp_core.loop.startable import Status

logger = getlogger()

# TODO: move it to plenum-util repo


class Prodable:
    """
    An interface for declaring classes that can be started and prodded. When an
    object is prodded, it just means that the event loop is giving it a chance
    to do something.
    """

    def name(self):
        raise NotImplementedError("subclass {} should implement this method"
                                  .format(self))

    async def prod(self, limit) -> int:
        """
        Action to be performed each time the Prodable object gets processor
        resources.

        :param limit: the number of messages to be processed
        """
        raise NotImplementedError("subclass {} should implement this method"
                                  .format(self))

    def start(self, loop):
        """
        Actions to be performed when the Prodable is starting up.
        """
        raise NotImplementedError("subclass {} should implement this method"
                                  .format(self))

    def stop(self):
        """
        Actions to be performed when the Prodable is starting up.
        """
        raise NotImplementedError("subclass {} should implement this method"
                                  .format(self))

    def get_status(self) -> Status:
        """
        Get the current status of this Prodable
        """
        raise NotImplementedError("subclass {} should implement this method"
                                  .format(self))


class Looper:
    """
    A helper class for asyncio's event_loop
    """

    def __init__(self,
                 prodables: List[Prodable]=None,
                 loop=None,
                 debug=False,
                 autoStart=True):
        """
        Initialize looper with an event loop.

        :param prodables: a list of prodables that this event loop will execute
        :param loop: the event loop to use
        :param debug: set_debug on event loop will be set to this value
        :param autoStart: start immediately?
        """
        self.prodables = list(prodables) if prodables is not None \
            else []  # type: List[Prodable]

        # if sys.platform == 'linux':
        #     asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        if loop:
            self.loop = loop
        else:
            try:
                # if sys.platform == 'win32':
                #    loop = asyncio.ProactorEventLoop()
                #    asyncio.set_event_loop(loop)
                evl = asyncio.get_event_loop()
                if evl.is_closed():
                    raise RuntimeError("event loop was closed")
            except Exception as ex:
                logger.debug("Looper could not get default event loop; "
                             "creating a new one: {}".format(ex))
                # Trying out uvloop for linux
                evl = asyncio.new_event_loop()
            asyncio.set_event_loop(evl)
            self.loop = evl

        logger.info("Starting up indy-node")
        self.runFut = self.loop.create_task(self.runForever())  # type: Task
        self.running = True  # type: bool

        self.loop.set_debug(bool(int(os.getenv('PYTHONASYNCIODEBUG', debug))))

        # TODO: uncomment this when python bug fixed (not just closed, but solved!)
        # https://bugs.python.org/issue23548
        #
        # signals = [item for item in dir(signal)
        #             if item.startswith("SIG") and item[3] != "_"]

        self.signals = ["SIGINT", "SIGTERM"]

        setSignal = \
            signal.signal if sys.platform == 'win32' \
            else self.loop.add_signal_handler

        for sigName in self.signals:
            try:
                logger.debug("Setting handler for {}".format(sigName))
                sigNum = getattr(signal, sigName)
                setSignal(sigNum, self.handleSignal)
            except RuntimeError as e:
                logger.debug(
                    "Cannot set handler for {} because {}".format(sigName, e))

        self.autoStart = autoStart  # type: bool
        if self.autoStart:
            self.startall()

    # @staticmethod
    # def new_event_loop():
    #     eventLib = asyncio if sys.platform == 'win32' else uvloop
    #     return eventLib.new_event_loop()

    async def prodAllOnce(self):
        """
        Call `prod` once for each Prodable in this Looper

        :return: the sum of the number of events executed successfully
        """
        # TODO: looks like limit is always None???
        limit = None
        s = 0
        for n in self.prodables:
            s += await n.prod(limit)
        return s

    def add(self, prodable: Prodable) -> None:
        """
        Add one Prodable object to this Looper's list of Prodables

        :param prodable: the Prodable object to add
        """
        if prodable.name in [p.name for p in self.prodables]:
            raise ProdableAlreadyAdded("Prodable {} already added.".
                                       format(prodable.name))
        self.prodables.append(prodable)
        if self.autoStart:
            prodable.start(self.loop)

    def removeProdable(self, prodable: Prodable=None, name: str=None) -> Optional[Prodable]:
        """
        Remove the specified Prodable object from this Looper's list of Prodables

        :param prodable: the Prodable to remove
        """
        if prodable:
            self.prodables.remove(prodable)
            return prodable
        elif name:
            for p in self.prodables:
                if hasattr(p, "name") and getattr(p, "name") == name:
                    prodable = p
                    break
            if prodable:
                self.prodables.remove(prodable)
                return prodable
            else:
                logger.warning("Trying to remove a prodable {} which is not present"
                               .format(prodable))
        else:
            logger.error("Provide a prodable object or a prodable name")

    def hasProdable(self, prodable: Prodable=None, name: str=None) -> bool:
        if not lxor(prodable, name):
            raise ValueError(
                "One and only one of prodable or name must be provided, "
                "passed {} and {}".format(prodable, name)
            )

        for p in self.prodables:
            if (prodable and p == prodable) or (name and name == p.name):
                return True

        return False

    async def runOnceNicely(self):
        """
        Execute `runOnce` with a small tolerance of 0.01 seconds so that the Prodables
        can complete their other asynchronous tasks not running on the event-loop.
        """
        start = time.perf_counter()
        msgsProcessed = await self.prodAllOnce()
        if msgsProcessed == 0:
            # if no let other stuff run
            await asyncio.sleep(0.01, loop=self.loop)
        dur = time.perf_counter() - start
        if dur >= 0.5:
            logger.debug("it took {:.3f} seconds to run once nicely".
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
                        raise RuntimeError(
                            "don't know how to run {}".format(coro))
                except Exception as ex:
                    logger.error("Error while running coroutine {}: {}".format(coro.__name__, ex.__repr__()))
                    raise ex
            if len(results) == 1:
                return results[0]
            return results
        if coros:
            what = wrapper()
        else:
            # if no coros supplied, then assume we run forever
            what = self.runFut
        return self.loop.run_until_complete(what)

    def handleSignal(self, sig=None):
        # Allowing sig to be optional since asyncio not passing the signal or
        # KeyboardInterrupt (Ctrl+C)
        logger.display("Signal {} received, stopping looper...".format(sig))
        self.running = False

    async def shutdown(self):
        """
        Shut down this Looper.
        """
        logger.display("Looper shutting down now...", extra={"cli": False})
        self.running = False
        start = time.perf_counter()
        if not self.runFut.done():
            await self.runFut
        self.stopall()
        logger.display("Looper shut down in {:.3f} seconds.".
                       format(time.perf_counter() - start), extra={"cli": False})
        # Unset signal handlers, bug: https://bugs.python.org/issue23548
        for sig_name in self.signals:
            logger.debug("Unsetting handler for {}".format(sig_name))
            sig_num = getattr(signal, sig_name)
            self.loop.remove_signal_handler(sig_num)

    def __enter__(self):
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
        Start all the Prodables in this Looper's `prodables`
        """
        for n in self.prodables:
            n.start(self.loop)

    def stopall(self):
        """
        Stop all the Prodables in this Looper's `prodables`
        """
        for n in self.prodables:
            n.stop()
