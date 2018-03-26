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

from common.error import error
from stp_core.common.log import getlogger

logger = getlogger()

# by allowing only primitives, it ensures we're signing the whole message
acceptableTypes = (str, int, float, list, dict, type(None))


class SigningSerializer:
    def serialize(self, obj, level=0, objname=None, topLevelKeysToIgnore=None,
                  toBytes=True):
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
        res = None
        if not isinstance(obj, acceptableTypes):
            error("invalid type found {}: {}".format(objname, obj))
        elif isinstance(obj, str):
            res = obj
        elif isinstance(obj, dict):
            if level > 0:
                keys = list(obj.keys())
            else:
                topLevelKeysToIgnore = topLevelKeysToIgnore or []
                keys = [k for k in obj.keys() if k not in topLevelKeysToIgnore]
            keys.sort()
            strs = []
            for k in keys:
                onm = ".".join([objname, k]) if objname else k
                strs.append(
                    str(k) + ":" + self.serialize(obj[k], level + 1, onm, toBytes=False))
            res = "|".join(strs)
        elif isinstance(obj, Iterable):
            strs = []
            for o in obj:
                strs.append(self.serialize(
                    o, level + 1, objname, toBytes=False))
            res = ",".join(strs)
        elif obj is None:
            res = ""
        else:
            res = str(obj)

        # logger.trace("serialized msg {} into {}".format(obj, res))

        if not toBytes:
            return res

        return res.encode('utf-8')

        # topLevelKeysToIgnore = topLevelKeysToIgnore or []
        # return ujson.dumps({k:obj[k] for k in obj.keys() if k not in topLevelKeysToIgnore}, sort_keys=True)
