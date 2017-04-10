"""

<<REQUEST, o, rid, c> σc, c>~μc
o = requested operation
rid or s = request identifier/ sequence number, should be in strictly increasing order
c = client id
σc = signed by c
μp,r = MAC appropriate for principals p and r
~μc = MAC authenticator, appropriate for all nodes


Requests:

<<REQUEST, o, rid, c>σc, c>~µc

<PROPAGATE, <REQUEST, o, s, c>σc, i>~µi

On reception of a PROPAGATE
message coming from node j, node i first verifies the MAC
authenticator.

"""
from collections import Iterable
from typing import Mapping

from stp_core.common.log import getlogger
from plenum.common.types import f
from plenum.common.error import error


logger = getlogger()

# by allowing only primitives, it ensures we're signing the whole message
acceptableTypes = (str, int, float, list, dict, type(None))


def serialize(obj, level=0, objname=None, topLevelKeysToIgnore=None):
    """
    Create a string representation of the given object.

    Examples:
    ::
    >>> serialize("str")
    'str'
    >>> serialize([1,2,3,4,5])
    '1,2,3,4,5'
    >>> signing.serlize({1:'a', 2:'b'})
    '1:a|2:b'
    >>> signing.serlize({1:'a', 2:'b', 3:[1,{2:'k'}]})
    '1:a|2:b|3:1,2:k'

    :param obj: the object to serlize
    :param level: a parameter used internally for recursion to serialize nested
     data structures
     :param topLevelKeysToIgnore: the list of top level keys to ignore for
     serialization
    :return: a string representation of `obj`
    """
    if not isinstance(obj, acceptableTypes):
        error("invalid type found {}: {}".format(objname, obj))
    if isinstance(obj, str):
        return obj
    if isinstance(obj, dict):
        if level > 0:
            keys = list(obj.keys())
        else:
            topLevelKeysToIgnore = topLevelKeysToIgnore or []
            keys = [k for k in obj.keys() if k not in topLevelKeysToIgnore]
        keys.sort()
        strs = []
        for k in keys:
            onm = ".".join([objname, k]) if objname else k
            strs.append(str(k) + ":" + serialize(obj[k], level + 1, onm))
        return "|".join(strs)
    if isinstance(obj, Iterable):
        strs = []
        for o in obj:
            strs.append(serialize(o, level + 1, objname))
        return ",".join(strs)
    if obj is None:
        return ""
    else:
        return str(obj)
    # topLevelKeysToIgnore = topLevelKeysToIgnore or []
    # return ujson.dumps({k:obj[k] for k in obj.keys() if k not in topLevelKeysToIgnore}, sort_keys=True)


def serializeMsg(msg: Mapping, topLevelKeysToIgnore=None):
    """
    Serialize a message for signing.

    :param msg: the message to sign
    :param topLevelKeysToIgnore: the top level keys of the Mapping that should
    not be included in the serialized form
    :return: a uft-8 encoded version of `msg`
    """
    ser = serialize(msg, topLevelKeysToIgnore=topLevelKeysToIgnore)
    logger.trace("serialized msg {} into {}".format(msg, ser))
    return ser.encode('utf-8')
