import asyncio
import time
from asyncio.coroutines import CoroWrapper
from inspect import isawaitable
from typing import Callable, TypeVar, Optional, Iterable

from zeno.common.util import getlogger

from zeno.common.ratchet import Ratchet

T = TypeVar('T')

logger = getlogger()

FlexFunc = TypeVar('flexFunc', CoroWrapper, Callable[[], T])

async def eventuallySoon(coroFunc: FlexFunc, *args):
    return await eventually(coroFunc, *args,
                            retryWait=0.1,
                            timeout=3,
                            ratchetSteps=10)


async def eventuallyAll(*coroFuncs: FlexFunc, # (use functools.partials if needed)
                        totalTimeout: float,
                        retryWait: float=0.1,
                        acceptableExceptions=None,
                        acceptableFails: int=0):
    """

    :param coroFuncs: iterable of no-arg functions
    :param totalTimeout:
    :param retryWait:
    :param acceptableExceptions:
    :param acceptableFails: how many of the passed in coroutines can ultimately fail and still be ok
    :return:
    """
    start = time.perf_counter()

    def remaining():
        return totalTimeout + start - time.perf_counter()
    funcNames = []
    others = 0
    fails = 0
    for cf in coroFuncs:
        if len(funcNames) < 2:
            funcNames.append(getFuncName(cf))
        else:
            others += 1
        # noinspection PyBroadException
        try:
            await eventually(cf,
                             retryWait=retryWait,
                             timeout=remaining(),
                             acceptableExceptions=acceptableExceptions,
                             verbose=False)
        except Exception:
            fails += 1
            logger.debug("a coro {} timed out without succeeding; fail count: "
                         "{}, acceptable: {}".
                         format(getFuncName(cf), fails, acceptableFails))
            if fails > acceptableFails:
                raise
    if others:
        funcNames.append("and {} others".format(others))
    desc = ", ".join(funcNames)
    logger.debug("{} succeeded with {:.2f} seconds to spare".
                 format(desc, remaining()))


def getFuncName(f):
    if hasattr(f, "__name__"):
        return f.__name__
    elif hasattr(f, "func"):
        return "partial({})".format(getFuncName(f.func))
    else:
        return "<unknown>"


async def eventually(coroFunc: FlexFunc,
                     *args,
                     retryWait: float=0.1,
                     timeout: float=5,
                     ratchetSteps: Optional[int]=None,
                     acceptableExceptions=None,
                     verbose=True) -> T:

    if acceptableExceptions and not isinstance(acceptableExceptions, Iterable):
            acceptableExceptions = [acceptableExceptions]
    start = time.perf_counter()

    ratchet = Ratchet.fromGoalDuration(retryWait, ratchetSteps, timeout).gen() if ratchetSteps else None

    def remaining():
        return start + timeout - time.perf_counter()

    fname = getFuncName(coroFunc)
    while True:
        try:
            remain = remaining()
            if remain < 0:
                # this provides a convenient breakpoint for a debugger
                logger.warning("{} last try...".format(fname))
            # noinspection PyCallingNonCallable
            res = coroFunc(*args)
            if isawaitable(res):
                res = await res
            if verbose:
                logger.debug("{} succeeded with {:.2f} seconds to spare".
                             format(fname, remain))
            return res
        except Exception as ex:
            if acceptableExceptions and type(ex) not in acceptableExceptions:
                raise
            if remain >= 0:
                if verbose:
                    logger.trace("{} not succeeded yet, {:.2f} seconds "
                                 "remaining...".format(fname, remain))
                await asyncio.sleep(next(ratchet) if ratchet else retryWait)
            else:
                logger.error("{} failed; not trying any more because {} "
                             "seconds have passed; args were {}".
                             format(fname, timeout, args))
                raise ex

