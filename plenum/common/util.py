import asyncio
import collections
import dateutil.tz
import functools
import glob
import inspect
import ipaddress
import itertools
import json
import logging
import math
import os
import random
import re
import time
from binascii import unhexlify, hexlify
from collections import Counter, defaultdict
from collections import OrderedDict
from datetime import datetime
from enum import unique, IntEnum
from math import floor
from typing import TypeVar, Iterable, Mapping, Set, Sequence, Any, Dict, \
    Tuple, Union, NamedTuple, Callable

import base58
import libnacl.secret
import psutil
from libnacl import randombytes, randombytes_uniform
from six import iteritems, string_types
from sortedcontainers import SortedDict as _SortedDict
from zmq.utils import z85

from common.error import error
from common.exceptions import PlenumTypeError, PlenumValueError
from ledger.util import F
from stp_core.crypto.util import isHexKey, isHex
from stp_core.network.exceptions import \
    InvalidEndpointIpAddress, InvalidEndpointPort

# TODO Do not remove the next import until imports in indy are fixed
from stp_core.common.util import adict


T = TypeVar('T')
Seconds = TypeVar("Seconds", int, float)


def randomString(size: int = 20) -> str:
    """
    Generate a random string in hex of the specified size

    DO NOT use python provided random class its a Pseudo Random Number Generator
    and not secure enough for our needs

    :param size: size of the random string to generate
    :return: the hexadecimal random string
    """

    def randomStr(size):
        if not isinstance(size, int):
            raise PlenumTypeError('size', size, int)
        if not size > 0:
            raise PlenumValueError('size', size, '> 0')
        # Approach 1
        rv = randombytes(size // 2).hex()

        return rv if size % 2 == 0 else rv + hex(randombytes_uniform(15))[-1]

        # Approach 2 this is faster than Approach 1, but lovesh had a doubt
        # that part of a random may not be truly random, so until
        # we have definite proof going to retain it commented
        # rstr = randombytes(size).hex()
        # return rstr[:size]

    return randomStr(size)


def random_from_alphabet(size, alphabet):
    """
    Takes *size* random elements from provided alphabet
    :param size:
    :param alphabet:
    """
    import random
    return list(random.choice(alphabet) for _ in range(size))


def randomSeed(size=32):
    return randomString(size)


def mostCommonElement(elements: Iterable[T], to_hashable_f: Callable=None):
    """
    Find the most frequent element of a collection.

    :param elements: An iterable of elements
    :param to_hashable_f: (optional) if defined will be used to get
        hashable presentation for non-hashable elements. Otherwise json.dumps
        is used with sort_keys=True
    :return: element which is the most frequent in the collection and
        the number of its occurrences
    """
    class _Hashable(collections.abc.Hashable):
        def __init__(self, orig):
            self.orig = orig

            if isinstance(orig, collections.Hashable):
                self.hashable = orig
            elif to_hashable_f is not None:
                self.hashable = to_hashable_f(orig)
            else:
                self.hashable = json.dumps(orig, sort_keys=True)

        def __eq__(self, other):
            return self.hashable == other.hashable

        def __hash__(self):
            return hash(self.hashable)

    _elements = (_Hashable(el) for el in elements)
    most_common, counter = Counter(_elements).most_common(n=1)[0]
    return most_common.orig, counter


def updateNamedTuple(tupleToUpdate: NamedTuple, **kwargs):
    tplData = dict(tupleToUpdate._asdict())
    tplData.update(kwargs)
    return tupleToUpdate.__class__(**tplData)


def objSearchReplace(obj: Any,
                     toFrom: Dict[Any, Any],
                     checked: Set[Any]=None,
                     logMsg: str=None,
                     deepLevel: int=None) -> None:
    """
    Search for an attribute in an object and replace it with another.

    :param obj: the object to search for the attribute
    :param toFrom: dictionary of the attribute name before and after search and replace i.e. search for the key and replace with the value
    :param checked: set of attributes of the object for recursion. optional. defaults to `set()`
    :param logMsg: a custom log message
    """

    if checked is None:
        checked = set()

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
                    logging.debug(
                        "{}in object {}, attribute {} changed from {} to {}". format(
                            logMsg + ": " if logMsg else "", obj, nm, old, new))
                    if isinstance(obj, dict):
                        obj[nm] = new
                    else:
                        setattr(obj, nm, new)
                    mutated = True
            if not mutated:
                if deepLevel is not None and deepLevel == 0:
                    continue
                objSearchReplace(o, toFrom, checked, logMsg, deepLevel -
                                 1 if deepLevel is not None else deepLevel)
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


def getSymmetricallyEncryptedVal(
        val, secretKey: Union[str, bytes]=None) -> Tuple[str, str]:
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
        return int(floor((nodeCount - 1) / 3))
    else:
        return 0


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
    return base58.b58encode(raw).decode("utf-8")


def friendlyToRaw(f):
    return base58.b58decode(f)


def cryptonymToHex(cryptonym: str) -> bytes:
    return hexlify(base58.b58decode(cryptonym.encode()))


def z85_to_friendly(z):
    try:
        return z if isinstance(z, str) else hexToFriendly(hexlify(z85.decode(z)))
    except ValueError:
        return z


def runWithLoop(loop, callback, *args, **kwargs):
    if loop.is_running():
        loop.call_soon(asyncio.async, callback(*args, **kwargs))
    else:
        loop.run_until_complete(callback(*args, **kwargs))


def checkIfMoreThanFSameItems(items, maxF):
    # TODO: separate json serialization into serialization.py
    jsonified_items = [json.dumps(item, sort_keys=True) for item in items]
    counts = defaultdict(int)
    for j_item in jsonified_items:
        counts[j_item] += 1
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
        n.clientAuthNr.core_authenticator.addIdr(identifier, verkey)


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


def get_utc_datetime() -> datetime:
    """
    :return: current datetime in UTC with explicit UTC marker
    """
    return datetime.utcnow().replace(tzinfo=dateutil.tz.tzutc())


def get_utc_epoch() -> int:
    """
    :return: epoch in UTC
    """
    return int(get_utc_datetime().timestamp())


def get_datetime_from_ts(ts):
    """

    :param ts: timestamp
    :return: datetime in UTC
    """
    return datetime.fromtimestamp(ts, dateutil.tz.tzutc())


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


def is_network_port_valid(port):
    return port.isdigit() and 0 < int(port) < 65536


def is_network_ip_address_valid(ip_address):
    try:
        ipaddress.ip_address(ip_address)
    except ValueError:
        return False
    else:
        return True


def is_hostname_valid(hostname):
    # Taken from https://stackoverflow.com/a/2532344
    if len(hostname) > 255:
        return False
    if hostname[-1] == ".":
        hostname = hostname[:-1]    # strip exactly one dot from the right,
        # if present
    allowed = re.compile("(?!-)[A-Z\d-]{1,63}(?<!-)$", re.IGNORECASE)
    return all(allowed.match(x) for x in hostname.split("."))


def check_endpoint_valid(endpoint):
    if ':' not in endpoint:
        # TODO: replace with more suitable exception
        raise InvalidEndpointIpAddress(endpoint)
    ip, port = endpoint.split(':')
    if not is_network_ip_address_valid(ip):
        raise InvalidEndpointIpAddress(endpoint)
    if not is_network_port_valid(port):
        raise InvalidEndpointPort(endpoint)


def getOpenConnections():
    pr = psutil.Process(os.getpid())
    return pr.connections()


def getFormattedErrorMsg(msg):
    msgHalfLength = int(len(msg) / 2)
    errorLine = "-" * msgHalfLength + "ERROR" + "-" * msgHalfLength
    return "\n\n" + errorLine + "\n  " + msg + "\n" + errorLine + "\n"


def getWalletFilePath(basedir, walletFileName):
    return os.path.join(basedir, walletFileName)


def pop_keys(mapping: Dict, cond: Callable):
    rem = []
    for k in mapping:
        if cond(k):
            rem.append(k)
    for i in rem:
        mapping.pop(i)


def check_if_all_equal_in_list(lst):
    return lst.count(lst[0]) == len(lst)


def compare_3PC_keys(key1, key2) -> int:
    """
    Return >0 if key2 is greater than key1, <0 if lesser, 0 otherwise
    """
    return key2[1] - key1[1]


def min_3PC_key(keys) -> Tuple[int, int]:
    return min(keys, key=lambda k: k[1])


def max_3PC_key(keys) -> Tuple[int, int]:
    return max(keys, key=lambda k: k[1])


if 'peekitem' in dir(_SortedDict):
    SortedDict = _SortedDict
else:
    # Since older versions of `SortedDict` lack `peekitem`
    class SortedDict(_SortedDict):
        def peekitem(self, index=-1):
            # This method is copied from `SortedDict`'s source code
            """Return (key, value) item pair at index.

            Unlike ``popitem``, the sorted dictionary is not modified. Index
            defaults to -1, the last/greatest key in the dictionary. Specify
            ``index=0`` to lookup the first/least key in the dictiony.

            If index is out of range, raise IndexError.

            """
            key = self._list[index]
            return key, self[key]


@unique
class UniqueSet(IntEnum):
    @classmethod
    def get_all_vals(cls):
        return [i.value for i in cls.__members__.values()]
