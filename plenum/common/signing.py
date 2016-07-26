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
import ctypes
from collections import Iterable
from typing import Mapping

from libnacl import crypto_box_SECRETKEYBYTES, nacl, crypto_box_PUBLICKEYBYTES
from plenum.common.types import f
from plenum.common.util import error, getlogger

logger = getlogger()

# by allowing only primitives, it ensures we're signing the whole message
acceptableTypes = (str, int, float, list, dict, type(None))


def serlize(obj, level=0, objname=None):
    """
    Create a string representation of the given object.

    Examples:
    ::
    >>> serlize("str")
    'str'
    >>> serlize([1,2,3,4,5])
    '1,2,3,4,5'
    >>> signing.serlize({1:'a', 2:'b'})
    '1:a|2:b'
    >>> signing.serlize({1:'a', 2:'b', 3:[1,{2:'k'}]})
    '1:a|2:b|3:1,2:k'

    :param obj: the object to serlize
    :param level: a parameter used internally for recursion to serialize nested data structures
    :return: a string representation of `obj`
    """
    if not isinstance(obj, acceptableTypes):
        error("invalid type found {}: {}".format(objname, obj))
    if isinstance(obj, str):
        return obj
    if isinstance(obj, dict):
        keys = [k for k in obj.keys() if level > 0 or k != f.SIG.nm]  # remove signature if top level
        keys.sort()
        strs = []
        for k in keys:
            onm = ".".join([objname, k]) if objname else k
            strs.append(str(k) + ":" + serlize(obj[k], level+1, onm))
        return "|".join(strs)
    if isinstance(obj, Iterable):
        strs = []
        for o in obj:
            strs.append(serlize(o, level+1, objname))
        return ",".join(strs)
    if obj is None:
        return ""
    else:
        return str(obj)


def serializeForSig(msg: Mapping):
    """
    Serialize a message for signing.

    :param msg: the message to sign
    :return: a uft-8 encoded version of `msg`
    """
    ser = serlize(msg)
    logger.trace("serialized for signing {} into {}".format(msg, ser))
    return ser.encode('utf-8')


# TODO: Should probably have a module called crypto since the functions below
# are not related to signing nor are they general utilities so cant be put into
# util.py
def ed25519SkToCurve25519(sk):
    secretKey = ctypes.create_string_buffer(crypto_box_SECRETKEYBYTES)
    ret = nacl.crypto_sign_ed25519_sk_to_curve25519(secretKey, sk)
    if ret:
        raise Exception("error in converting ed22519 key to curve25519")
    return secretKey.raw


def ed25519PkToCurve25519(pk):
    publicKey = ctypes.create_string_buffer(crypto_box_PUBLICKEYBYTES)
    ret = nacl.crypto_sign_ed25519_pk_to_curve25519(publicKey, pk)
    if ret:
        raise Exception("error in converting ed22519 key to curve25519")
    return publicKey.raw
