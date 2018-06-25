import inspect
import time
from functools import wraps
from typing import Any, List, NamedTuple, Tuple, Optional, Iterable, Union, \
    Callable
from typing import Dict
import traceback

try:
    from plenum.test import NO_SPIES
except ImportError:
    pass

from plenum.common.util import objSearchReplace
from stp_core.common.log import getlogger

logger = getlogger()

Entry = NamedTuple('Entry', [('starttime', float),
                             ('endtime', float),
                             ('method', str),
                             ('params', Dict),
                             ('result', Any)])

SpyableMethod = Union[str, Callable]
SpyableMethods = Iterable[SpyableMethod]


class SpyLog(list):
    def getLast(self, method: SpyableMethod, required: bool = False) -> \
            Optional[Entry]:
        entry = None  # type: Optional[Entry]
        if callable(method):
            method = method.__name__
        try:
            entry = next(x for x in reversed(self) if x.method == method)
        except StopIteration:
            if required:
                raise RuntimeError(
                    "spylog entry for method {} not found".format(method))
        return entry

    def getAll(self, method: SpyableMethod) -> List[Entry]:
        if callable(method):
            method = method.__name__
        return list(reversed([x for x in self if x.method == method]))

    def getLastParam(self, method: str, paramIndex: int = 0) -> Any:
        return self.getLastParams(method)[paramIndex]

    def getLastParams(self, method: str, required: bool = True) -> Tuple:
        last = self.getLast(method, required)
        return last.params if last is not None else None

    def getLastResult(self, method: str, required: bool = True) -> Tuple:
        last = self.getLast(method, required)
        return last.result if last is not None else None

    def count(self, method: SpyableMethod) -> int:
        if callable(method):
            method = method.__name__
        return sum(1 for x in self if x.method == method)


def spy(func, is_init, should_spy, spy_log=None):
    sig = inspect.signature(func)

    # sets up spylog, but doesn't spy on init
    def init_only(self, *args, **kwargs):
        self.spylog = SpyLog()
        return func(self, *args, **kwargs)

    init_only.__name__ = func.__name__

    # sets up spylog, and also spys on init
    def init_wrap(self, *args, **kwargs):
        self.spylog = SpyLog()
        return wrap(self, *args, **kwargs)

    init_wrap.__name__ = func.__name__

    # wraps a function call
    @wraps(func)
    def wrap(self, *args, **kwargs):
        start = time.perf_counter()
        r = None
        try:
            r = func(self, *args, **kwargs)
        except Exception as ex:
            r = ex
            # logger.error(traceback.print_exc())
            raise
        finally:
            bound = sig.bind(self, *args, **kwargs)
            params = dict(bound.arguments)
            params.pop('self', None)
            used_log = self.spylog if hasattr(self, 'spylog') else spy_log
            used_log.append(Entry(start,
                                  time.perf_counter(),
                                  func.__name__,
                                  params,
                                  r))
        return r

    return wrap if not is_init else init_wrap if should_spy else init_only


def spyable(name: str = None, methods: SpyableMethods = None,
            deep_level: int = None):
    def decorator(clas):

        if 'NO_SPIES' in globals() and globals()['NO_SPIES']:
            # Since spylog consumes resources, benchmarking tests need to be
            # able to not have spyables, so they set a module global `NO_SPIES`,
            #  it's their responsibility to unset it
            logger.info(
                'NOT USING SPIES ON METHODS AS THEY ARE EXPLICITLY DISABLED')
            return clas

        nonlocal name
        name = name if name else "Spyable" + clas.__name__

        spyable_type = type(name, (clas,), {})
        morphed = {}  # type: Dict[Callable, Callable]
        matches = []
        for nm, func in [(method, getattr(clas, method))
                         for method in dir(clas)
                         if callable(getattr(clas, method))]:
            isInit = nm == "__init__"
            matched = (nm if methods and nm in methods else
            func if methods and func in methods else
            None)
            # if method was specified to be spied on or is `__init__` method
            # or is does not have name starting with `__`
            shouldSpy = bool(matched) if methods else (
                    not nm.startswith("__") or isInit)
            if shouldSpy or isInit:
                newFunc = spy(func, isInit, shouldSpy)
                morphed[func] = newFunc
                setattr(spyable_type, nm, newFunc)
                logger.debug("in {} added spy on {}".
                             format(spyable_type.__name__, nm))
            matches.append(matched)

        if methods:
            for m in methods:
                if m not in matches:
                    logger.warning(
                        "method {} not found, so no spy added".format(m),
                        extra={"cli": False})

        objSearchReplace(spyable_type, morphed,
                         logMsg="Applying spy remapping", deepLevel=deep_level)
        return spyable_type

    return decorator
