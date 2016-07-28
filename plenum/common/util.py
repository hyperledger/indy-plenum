import asyncio
import base64
import importlib.util
import inspect
import itertools
import logging
import math
import os
import random
import socket
import string
import sys
import time
from binascii import unhexlify
from collections import Counter
from collections import OrderedDict
from math import floor
from typing import TypeVar, Iterable, Mapping, Set, Sequence, Any, Dict, \
    Tuple, Union, List, NamedTuple

import libnacl.secret
from ioflo.base.consoling import getConsole, Console
from libnacl import crypto_hash_sha256
from six import iteritems, string_types

T = TypeVar('T')
Seconds = TypeVar("Seconds", int, float)
CONFIG = None


def randomString(size: int = 20, chars: str = string.ascii_letters + string.digits) -> str:
    """
    Generate a random string of the specified size.

    Ensure that the size is less than the length of chars as this function uses random.choice
    which uses random sampling without replacement.

    :param size: size of the random string to generate
    :param chars: the set of characters to use to generate the random string. Uses alphanumerics by default.
    :return: the random string generated
    """
    return ''.join(random.sample(chars, size))


def mostCommonElement(elements: Iterable[T]) -> T:
    """
    Find the most frequent element of a collection.

    :param elements: An iterable of elements
    :return: element of type T which is most frequent in the collection
    """
    return Counter(elements).most_common(1)[0][0]


def updateNamedTuple(tupleToUpdate: NamedTuple, **kwargs):
    tplData = dict(tupleToUpdate._asdict())
    tplData.update(kwargs)
    return tupleToUpdate.__class__(**tplData)


def objSearchReplace(obj: Any, toFrom: Dict[Any, Any], checked: Set[Any] = set(), logMsg: str = None) -> None:
    """
    Search for an attribute in an object and replace it with another.

    :param obj: the object to search for the attribute
    :param toFrom: dictionary of the attribute name before and after search and replace i.e. search for the key and replace with the value
    :param checked: set of attributes of the object for recursion. optional. defaults to `set()`
    :param logMsg: a custom log message
    """
    checked.add(id(obj))
    pairs = [(i, getattr(obj, i)) for i in dir(obj) if not i.startswith("__")]

    if isinstance(obj, Mapping):
        pairs += [x for x in iteritems(obj)]
    elif isinstance(obj, (Sequence, Set)) and not isinstance(obj, string_types):
        pairs += [x for x in enumerate(obj)]

    for nm, o in pairs:
        if id(o) not in checked:
            mutated = False
            for old, new in toFrom.items():
                if id(o) == id(old):
                    logging.debug("{}in object {}, attribute {} changed from {} to {}".
                                  format(logMsg + ": " if logMsg else "",
                                         obj, nm, old, new))
                    if isinstance(obj, dict):
                        obj[nm] = new
                    else:
                        setattr(obj, nm, new)
                    mutated = True
            if not mutated:
                objSearchReplace(o, toFrom, checked, logMsg)
    checked.remove(id(obj))


def error(msg: str) -> Exception:
    """
    Wrapper to get around Python's distinction between statements and expressions
    Can be used in lambdas and expressions such as: a if b else error(c)

    :param msg: error message
    """
    raise Exception(msg)


def getRandomPortNumber() -> int:
    """
    Get a random port between 8090 and 65530

    :return: a random port number
    """
    return random.randint(8090, 65530)


def isHex(val: str) -> bool:
    """
    Return whether the given str represents a hex value or not

    :param val: the string to check
    :return: whether the given str represents a hex value
    """
    if isinstance(val, bytes):
        try:
            val = val.decode()
        except:
            return False
    return isinstance(val, str) and all(c in string.hexdigits for c in val)


async def runall(corogen):
    """
    Run an array of coroutines

    :param corogen: a generator that generates coroutines
    :return: list or returns of the coroutines
    """
    results = []
    for c in corogen:
        result = await c
        results.append(result)
    return results


def getSymmetricallyEncryptedVal(val, secretKey: Union[str, bytes]=None) -> Tuple[str, str]:
    """
    Encrypt the provided value with symmetric encryption

    :param val: the value to encrypt
    :param secretKey: Optional key, if provided should be either in hex or bytes
    :return: Tuple of the encrypted value and secret key encoded in hex
    """
    if isinstance(val, str):
        val = val.encode("utf-8")
    if secretKey:
        if isHex(secretKey):
            secretKey = bytes(bytearray.fromhex(secretKey))
        elif not isinstance(secretKey, bytes):
            error("Secret key must be either in hex or bytes")
        box = libnacl.secret.SecretBox(secretKey)
    else:
        box = libnacl.secret.SecretBox()
    return box.encrypt(val).hex(), box.sk.hex()


def getMaxFailures(nodeCount: int) -> int:
    r"""
    The maximum number of Byzantine failures permissible by the RBFT system.
    Calculated as :math:`f = (N-1)/3`

    :param nodeCount: number of nodes in the system
    :return: maximum permissible Byzantine failures in the system
    """
    if nodeCount >= 4:
        return floor((nodeCount - 1) / 3)
    else:
        return 0


def getQuorum(nodeCount: int = None, f: int = None) -> int:
    r"""
    Return the minimum number of nodes where the number of correct nodes is
    greater than the number of faulty nodes.
    Calculated as :math:`2*f + 1`

    :param nodeCount: the number of nodes in the system
    :param f: the max. number of failures
    """
    if nodeCount is not None:
        f = getMaxFailures(nodeCount)
    if f is not None:
        return 2 * f + 1


def getNoInstances(nodeCount: int) -> int:
    """
    Return the number of protocol instances which is equal to f + 1. See
    `getMaxFailures`

    :param nodeCount: number of nodes
    :return: number of protocol instances
    """
    return getMaxFailures(nodeCount) + 1


TRACE_LOG_LEVEL = 5
DISPLAY_LOG_LEVEL = 25


class CustomAdapter(logging.LoggerAdapter):
    def trace(self, msg, *args, **kwargs):
        self.log(TRACE_LOG_LEVEL, msg, *args, **kwargs)

    def display(self, msg, *args, **kwargs):
        self.log(DISPLAY_LOG_LEVEL, msg, *args, **kwargs)


class CliHandler(logging.Handler):
    def __init__(self, callback):
        """
        Initialize the handler.
        """
        super().__init__()
        self.callback = callback

    def emit(self, record):
        """
        Passes the log record back to the CLI for rendering
        """
        if hasattr(record, "cli"):
            if record.cli:
                self.callback(record, record.cli)
        elif record.levelno >= logging.INFO:
            self.callback(record)


class DemoHandler(logging.Handler):
    def __init__(self, callback):
        """
        Initialize the handler.
        """
        super().__init__()
        self.callback = callback

    def emit(self, record):
        """
        Passes the log record back to the CLI for rendering
        """
        if hasattr(record, "demo"):
            if record.cli:
                self.callback(record, record.cli)
        elif record.levelno >= logging.INFO:
            self.callback(record)


loggingConfigured = False


def getlogger(name=None):
    if not loggingConfigured:
        setupLogging(TRACE_LOG_LEVEL)
    if not name:
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        name = inspect.getmodule(calframe[1][0]).__name__
    logger = logging.getLogger(name)
    return logger


class TestingHandler(logging.Handler):
    def __init__(self, tester):
        """
        Initialize the handler.
        """
        super().__init__()
        self.tester = tester

    def emit(self, record):
        """
        Captures a record.
        """
        self.tester(record)


def setupLogging(log_level, raet_log_level=None, filename=None):
    """
    Setup for logging.
    log level is TRACE by default.
    """
    if filename:
        d = os.path.dirname(filename)
        if not os.path.exists(d):
            os.makedirs(d)

    addTraceToLogging()
    addDisplayToLogging()

    if filename:
        mode = 'w'
        h = logging.FileHandler(filename, mode)

    else:
        h = logging.StreamHandler(sys.stdout)
    handlers = [h]
    log_format = '{relativeCreated:,.0f} {levelname:7s} {message:s}'
    fmt = logging.Formatter(log_format, None, style='{')
    for h in handlers:
        if h.formatter is None:
            h.setFormatter(fmt)
        logging.root.addHandler(h)
    logging.root.setLevel(log_level)

    console = getConsole()
    verbosity = raet_log_level \
        if raet_log_level is not None \
        else Console.Wordage.terse
    console.reinit(verbosity=verbosity)
    global loggingConfigured
    loggingConfigured = True


def addTraceToLogging():
    logging.addLevelName(TRACE_LOG_LEVEL, "TRACE")

    def trace(self, message, *args, **kwargs):
        if self.isEnabledFor(TRACE_LOG_LEVEL):
            self._log(TRACE_LOG_LEVEL, message, args, **kwargs)

    logging.Logger.trace = trace


def addDisplayToLogging():
    logging.addLevelName(DISPLAY_LOG_LEVEL, "DISPLAY")

    def display(self, message, *args, **kwargs):
        if self.isEnabledFor(DISPLAY_LOG_LEVEL):
            self._log(DISPLAY_LOG_LEVEL, message, args, **kwargs)

    logging.Logger.display = display


def prime_gen() -> int:
    # credit to David Eppstein, Wolfgang Beneicke, Paul Hofstra
    """
    A generator for prime numbers starting from 2.
    """
    D = {}
    yield 2
    for q in itertools.islice(itertools.count(3), 0, None, 2):
        p = D.pop(q, None)
        if p is None:
            D[q * q] = 2 * q
            yield q
        else:
            x = p + q
            while x in D:
                x += p
            D[x] = p


def evenCompare(a: str, b: str) -> bool:
    """
    A deterministic but more evenly distributed comparator than simple alphabetical.
    Useful when comparing consecutive strings and an even distribution is needed.
    Provides an even chance of returning true as often as false
    """
    ab = a.encode('utf-8')
    bb = b.encode('utf-8')
    ac = crypto_hash_sha256(ab)
    bc = crypto_hash_sha256(bb)
    return ac < bc


def distributedConnectionMap(names: List[str]) -> OrderedDict:
    """
    Create a map where every node is connected every other node.
    Assume each key in the returned dictionary to be connected to each item in
    its value(list).

    :param names: a list of node names
    :return: a dictionary of name -> list(name).
    """
    names.sort()
    combos = list(itertools.combinations(names, 2))
    maxPer = math.ceil(len(list(combos)) / len(names))
    # maxconns = math.ceil(len(names) / 2)
    connmap = OrderedDict((n, []) for n in names)
    for a, b in combos:
        if len(connmap[a]) < maxPer:
            connmap[a].append(b)
        else:
            connmap[b].append(a)
    return connmap


def checkPortAvailable(ha) -> bool:
    """Returns whether the given port is available"""
    available = True
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.bind(ha)
    except:
        logging.warning("Checked port availability for opening "
                        "and address was already in use: {}".format(ha))
        available = False
    finally:
        sock.close()
    return available


class MessageProcessor:
    """
    Helper functions for messages.
    """

    def discard(self, msg, reason, logMethod=logging.error, cliOutput=False):
        """
        Discard a message and log a reason using the specified `logMethod`.

        :param msg: the message to discard
        :param reason: the reason why this message is being discarded
        :param logMethod: the logging function to be used
        """
        reason = "" if not reason else " because {}".format(reason)
        logMethod("{} discarding message {}{}".format(self, msg, reason),
                  extra={"cli": cliOutput})


class adict(dict):
    """Dict with attr access to keys."""
    marker = object()

    def __init__(self, **kwargs):
        super().__init__()
        for key in kwargs:
            self.__setitem__(key, kwargs[key])

    def __setitem__(self, key, value):
        if isinstance(value, dict) and not isinstance(value, adict):
            value = adict(**value)
        super(adict, self).__setitem__(key, value)

    def __getitem__(self, key):
        found = self.get(key, adict.marker)
        if found is adict.marker:
            found = adict()
            super(adict, self).__setitem__(key, found)
        return found

    __setattr__ = __setitem__
    __getattr__ = __getitem__


def getInstalledConfig(installDir, configFile):
    """
    Reads config from the installation directory of Plenum.

    :param installDir: installation directory of Plenum
    :param configFile: name of the confiuration file
    :raises: FileNotFoundError
    :return: the configuration as a python object
    """
    configPath = os.path.join(installDir, configFile)
    if os.path.exists(configPath):
        spec = importlib.util.spec_from_file_location(configFile,
                                                      configPath)
        config = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config)
        return config
    else:
        raise FileNotFoundError("No file found at location {}".format(configPath))


def getConfig():
    """
    Reads a file called config.py in the project directory

    :raises: FileNotFoundError
    :return: the configuration as a python object
    """
    global CONFIG
    if not CONFIG:
        refConfig = importlib.import_module("plenum.config")
        try:
            homeDir = os.path.expanduser("~")
            configDir = os.path.join(homeDir, ".plenum")
            config = getInstalledConfig(configDir, "plenum_config.py")
            refConfig.__dict__.update(config.__dict__)
        except FileNotFoundError:
            pass
        CONFIG = refConfig
    return CONFIG


async def untilTrue(condition, *args, timeout=5) -> bool:
    """
    Keep checking the condition till it is true or a timeout is reached

    :param condition: the condition to check (a function that returns bool)
    :param args: the arguments to the condition
    :return: True if the condition is met in the given timeout, False otherwise
    """
    result = False
    start = time.perf_counter()
    elapsed = 0
    while elapsed < timeout:
        result = condition(*args)
        if result:
            break
        await asyncio.sleep(.1)
        elapsed = time.perf_counter() - start
    return result


def hasKeys(data, keynames):
    """
    Checks whether all keys are present in the given data, and are not None
    """
    # if all keys in `keynames` are not present in `data`
    if len(set(keynames).difference(set(data.keys()))) != 0:
        return False
    for key in keynames:
        if data[key] is None:
            return False
    return True


def firstKey(d: Dict):
    return next(iter(d.keys()))


def firstValue(d: Dict):
    return next(iter(d.values()))


def seedFromHex(seed):
    if len(seed) == 64:
        try:
            return unhexlify(seed)
        except:
            pass


def cleanSeed(seed=None):
    if seed:
        bts = seedFromHex(seed)
        if not bts:
            if isinstance(seed, str):
                seed = seed.encode('utf-8')
            bts = bytes(seed)
            if len(seed) != 32:
                error('seed length must be 32 bytes')
        return bts


def hexToCryptonym(hex):
    if isinstance(hex, str):
        hex = hex.encode()
    return base64.b64encode(unhexlify(hex)).decode()
