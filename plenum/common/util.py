import asyncio
import collections
import glob
import inspect
import ipaddress
import itertools
import json
import logging
import os
import random
import string
import time
import math
from binascii import unhexlify, hexlify
from collections import Counter
from collections import OrderedDict
from math import floor
from os.path import basename
from typing import TypeVar, Iterable, Mapping, Set, Sequence, Any, Dict, \
    Tuple, Union, NamedTuple, Callable

import base58
import libnacl.secret
import psutil
from jsonpickle import encode, decode
from six import iteritems, string_types

from ledger.util import F
from plenum.cli.constants import WALLET_FILE_EXTENSION
from plenum.common.error import error
from stp_core.crypto.util import isHexKey, isHex
from stp_core.network.exceptions import \
    MissingEndpoint, \
    InvalidEndpointIpAddress, InvalidEndpointPort
import functools


T = TypeVar('T')
Seconds = TypeVar("Seconds", int, float)


def randomString(size: int = 20,
                 chars: str = string.ascii_letters + string.digits) -> str:
    """
    Generate a random string of the specified size.

    Ensure that the size is less than the length of chars as this function uses random.choice
    which uses random sampling without replacement.

    :param size: size of the random string to generate
    :param chars: the set of characters to use to generate the random string. Uses alphanumerics by default.
    :return: the random string generated
    """
    assert size < len(chars), 'size should be less than the number of characters'
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


def objSearchReplace(obj: Any, toFrom: Dict[Any, Any], checked: Set[Any] = set()
                     , logMsg: str = None, deepLevel: int = None) -> None:
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
                if deepLevel is not None and deepLevel == 0:
                    continue
                objSearchReplace(o, toFrom, checked, logMsg, deepLevel - 1 if deepLevel is not None else deepLevel)
    checked.remove(id(obj))


def getRandomPortNumber() -> int:
    """
    Get a random port between 8090 and 65530

    :return: a random port number
    """
    return random.randint(8090, 65530)


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


def totalConnections(nodeCount: int) -> int:
    """
    :return: number of connections between nodes
    """
    return math.ceil((nodeCount * (nodeCount - 1)) / 2)


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

    def copy(self):
        return self.__copy__()

    def __copy__(self):
        return adict(**self)

    __setattr__ = __setitem__
    __getattr__ = __getitem__


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


def firstKey(d: Dict):
    return next(iter(d.keys()))


def firstValue(d: Dict):
    return next(iter(d.values()))


def getCryptonym(identifier):
    return base58.b58encode(unhexlify(identifier.encode())).decode() \
        if isHexKey(identifier) else identifier


def getFriendlyIdentifier(dest):
    return hexToFriendly(dest) if isHexKey(dest) else dest


def hexToFriendly(hx):
    if isinstance(hx, str):
        hx = hx.encode()
    raw = unhexlify(hx)
    return rawToFriendly(raw)


def friendlyToHex(f):
    if not isinstance(f, str):
        f = f.decode('ascii')
    raw = friendlyToRaw(f)
    return hexlify(raw)


def friendlyToHexStr(f):
    return friendlyToHex(f).decode()


def rawToFriendly(raw):
    return base58.b58encode(raw)


def friendlyToRaw(f):
    return base58.b58decode(f)


def cryptonymToHex(cryptonym: str) -> bytes:
    return hexlify(base58.b58decode(cryptonym.encode()))


def runWithLoop(loop, callback, *args, **kwargs):
    if loop.is_running():
        loop.call_soon(asyncio.async, callback(*args, **kwargs))
    else:
        loop.run_until_complete(callback(*args, **kwargs))


def checkIfMoreThanFSameItems(items, maxF):
    jsonifiedItems = [json.dumps(item, sort_keys=True) for item in items]
    counts = {}
    for jItem in jsonifiedItems:
        counts[jItem] = counts.get(jItem, 0) + 1
    if counts and counts[max(counts, key=counts.get)] > maxF:
        return json.loads(max(counts, key=counts.get))
    else:
        return False


def friendlyEx(ex: Exception) -> str:
    curEx = ex
    friendly = ""
    end = ""
    while curEx:
        if len(friendly):
            friendly += " [caused by "
            end += "]"
        friendly += "{}".format(curEx)
        curEx = curEx.__cause__
    friendly += end
    return friendly


def updateFieldsWithSeqNo(fields):
    r = OrderedDict()
    r[F.seqNo.name] = (str, int)
    r.update(fields)
    return r


def compareNamedTuple(tuple1: NamedTuple, tuple2: NamedTuple, *fields):
    """
    Compare provided fields of 2 named tuples for equality and returns true
    :param tuple1:
    :param tuple2:
    :param fields:
    :return:
    """
    tuple1 = tuple1._asdict()
    tuple2 = tuple2._asdict()
    comp = []
    for field in fields:
        comp.append(tuple1[field] == tuple2[field])
    return all(comp)


def bootstrapClientKeys(identifier, verkey, nodes):
    # bootstrap client verification key to all nodes
    for n in nodes:
        n.clientAuthNr.addIdr(identifier, verkey)


def prettyDateDifference(startTime, finishTime=None):
    """
    Get a datetime object or a int() Epoch timestamp and return a
    pretty string like 'an hour ago', 'Yesterday', '3 months ago',
    'just now', etc
    """
    from datetime import datetime

    if startTime is None:
        return None

    if not isinstance(startTime, (int, datetime)):
        raise RuntimeError("Cannot parse time")

    endTime = finishTime or datetime.now()

    if isinstance(startTime, int):
        diff = endTime - datetime.fromtimestamp(startTime)
    elif isinstance(startTime, datetime):
        diff = endTime - startTime
    else:
        diff = endTime - endTime

    second_diff = diff.seconds
    day_diff = diff.days

    if day_diff < 0:
        return ''

    if day_diff == 0:
        if second_diff < 10:
            return "just now"
        if second_diff < 60:
            return str(second_diff) + " seconds ago"
        if second_diff < 120:
            return "a minute ago"
        if second_diff < 3600:
            return str(int(second_diff / 60)) + " minutes ago"
        if second_diff < 7200:
            return "an hour ago"
        if second_diff < 86400:
            return str(int(second_diff / 3600)) + " hours ago"
    if day_diff == 1:
        return "Yesterday"
    if day_diff < 7:
        return str(day_diff) + " days ago"


INITIAL_WALL_TIME = time.time()


INITIAL_PERF_COUNTER = time.perf_counter()


def preciseTime():
    perfCounterDelta = time.perf_counter() - INITIAL_PERF_COUNTER
    return INITIAL_WALL_TIME + perfCounterDelta


TIME_BASED_REQ_ID_PRECISION = 1000000


def getTimeBasedId():
    return int(preciseTime() * TIME_BASED_REQ_ID_PRECISION)


def convertTimeBasedReqIdToMillis(reqId):
    return (reqId / TIME_BASED_REQ_ID_PRECISION) * 1000


def isMaxCheckTimeExpired(startTime, maxCheckForMillis):
    curTimeRounded = round(time.time() * 1000)
    startTimeRounded = round(startTime * 1000)
    return startTimeRounded + maxCheckForMillis < curTimeRounded


def randomSeed(size=32):
    return ''.join(random.choice(string.hexdigits)
                   for _ in range(size)).encode()


def lxor(a, b):
    # Logical xor of 2 items, return true when one of them is truthy and
    # one of them falsy
    return bool(a) != bool(b)


def getCallableName(callable: Callable):
    # If it is a function or method then access its `__name__`
    if inspect.isfunction(callable) or \
            inspect.ismethod(callable) or \
            isinstance(callable, functools.partial):

        if hasattr(callable, "__name__"):
            return callable.__name__
        # If it is a partial then access its `func`'s `__name__`
        elif hasattr(callable, "func"):
            return callable.func.__name__
        else:
            RuntimeError("Do not know how to get name of this callable")
    else:
        TypeError("This is not a callable")


def updateNestedDict(d, u, nestedKeysToUpdate=None):
    for k, v in u.items():
        if isinstance(v, collections.Mapping) and \
                (not nestedKeysToUpdate or k in nestedKeysToUpdate):
            r = updateNestedDict(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d


def createDirIfNotExists(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)


def is_valid_port(port):
    return port.isdigit() and int(port) in range(1, 65536)


def check_endpoint_valid(endpoint, required: bool=True):
    if not endpoint:
        if required:
            raise MissingEndpoint()
        else:
            return
    ip, port = endpoint.split(':')
    try:
        ipaddress.ip_address(ip)
    except Exception as exc:
        raise InvalidEndpointIpAddress(endpoint) from exc
    if not is_valid_port(port):
        raise InvalidEndpointPort(endpoint)


def getOpenConnections():
    pr = psutil.Process(os.getpid())
    return pr.connections()


def getFormattedErrorMsg(msg):
    msgHalfLength = int(len(msg) / 2)
    errorLine = "-" * msgHalfLength + "ERROR" + "-" * msgHalfLength
    return "\n\n" + errorLine + "\n  " + msg + "\n" + errorLine + "\n"

def normalizedWalletFileName(walletName):
    return "{}.{}".format(walletName.lower(), WALLET_FILE_EXTENSION)


def getWalletFilePath(basedir, walletFileName):
    return os.path.join(basedir, walletFileName)


def saveGivenWallet(wallet, fileName, contextDir):
    createDirIfNotExists(contextDir)
    walletFilePath = getWalletFilePath(
        contextDir, fileName)
    with open(walletFilePath, "w+") as walletFile:
        encodedWallet = encode(wallet, keys=True)
        walletFile.write(encodedWallet)
    return walletFilePath


def getWalletByPath(walletFilePath):
    with open(walletFilePath) as walletFile:
        wallet = decode(walletFile.read(), keys=True)
        return wallet


def getLastSavedWalletFileName(dir):
    def getLastModifiedTime(file):
        return os.stat(file).st_mtime_ns

    filePattern = "*.{}".format(WALLET_FILE_EXTENSION)
    newest = max(glob.iglob('{}/{}'.format(dir, filePattern)),
                 key=getLastModifiedTime)
    return basename(newest)
