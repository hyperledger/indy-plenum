import inspect
import logging
import time
from functools import wraps
from typing import Any, List, NamedTuple, Tuple, Optional, Iterable, Union, \
    Callable
from typing import Dict

from plenum.common.util import objSearchReplace
from plenum.common.log import getlogger

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

    def getAll(self, method: str) -> List[Entry]:
        return list(reversed([x for x in self if x.method == method]))

    def getLastParam(self, method: str, paramIndex: int = 0) -> Any:
        return self.getLastParams(method)[paramIndex]

    def getLastParams(self, method: str, required: bool = True) -> Tuple:
        last = self.getLast(method, required)
        return last.params if last is not None else None

    def count(self, method: str) -> int:
        return sum(1 for x in self if x.method == method)


def Spyable(name: str = None, methods: SpyableMethods = None, deepLevel: int = None):
    def spy(func, isInit, shouldSpy):

        sig = inspect.signature(func)
        paramNames = [k for k in sig.parameters]
        # TODO Find a better way
        if paramNames and paramNames[0] == "self":
            paramNames = paramNames[1:]

        # sets up spylog, but doesn't spy on init
        def initOnly(self, *args, **kwargs):
            self.spylog = SpyLog()
            return func(self, *args, **kwargs)

        initOnly.__name__ = func.__name__

        # sets up spylog, and also spys on init
        def initWrap(self, *args, **kwargs):
            self.spylog = SpyLog()
            return wrap(self, *args, **kwargs)

        initWrap.__name__ = func.__name__

        # wraps a function call
        @wraps(func)
        def wrap(self, *args, **kwargs):
            start = time.perf_counter()
            r = None
            try:
                r = func(self, *args, **kwargs)
            except Exception as ex:
                r = ex
                raise
            finally:
                params = {}
                if kwargs:
                    for k, v in kwargs.items():
                        params[k] = v
                if args:
                    for i, nm in enumerate(paramNames[:len(args)]):
                        params[nm] = args[i]

                self.spylog.append(Entry(start,
                                         time.perf_counter(),
                                         func.__name__,
                                         params,
                                         r))
            return r

        return wrap if not isInit else initWrap if shouldSpy else initOnly

    def decorator(clas):
        nonlocal name
        name = name if name else "Spyable" + clas.__name__

        spyable = type(name, (clas,), {})
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
                setattr(spyable, nm, newFunc)
                logger.debug("in {} added spy on {}".
                              format(spyable.__name__, nm))
            matches.append(matched)

        if methods:
            for m in methods:
                if m not in matches:
                    logger.warning(
                        "method {} not found, so no spy added".format(m),
                        extra={"cli": False})

        objSearchReplace(spyable, morphed, logMsg="Applying spy remapping", deepLevel=deepLevel)
        return spyable

    return decorator
