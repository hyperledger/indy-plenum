from collections import abc
from collections import deque
from functools import wraps

import sys
import time
from typing import Optional, Tuple, List


def get_size(obj, seen=None):
    """Recursively finds size of objects"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size


def timeit(method, record_time_in: Optional[List]=None):
    @wraps(method)
    def timed(*args, **kw):
        nonlocal method
        ts = time.perf_counter()
        result = method(*args, **kw)
        te = time.perf_counter()
        elapsed = te - ts
        # print('{} took {} sec'.format(method.__name__, elapsed))
        try:
            method.elapsed = elapsed
        # setattr(method, 'elapsed', elapsed)
        except AttributeError:
            pass
        if record_time_in is not None:
            record_time_in.append(elapsed)
        return result

    return timed


def get_collection_sizes(obj, collections: Optional[Tuple]=None,
                         get_only_non_empty=False):
    """
    Iterates over `collections` of the gives object and gives its byte size
    and number of items in collection
    """
    from pympler import asizeof
    collections = collections or (list, dict, set, deque, abc.Sized)
    if not isinstance(collections, tuple):
        collections = tuple(collections)

    result = []
    for attr_name in dir(obj):
        attr = getattr(obj, attr_name)
        if isinstance(attr, collections) and (
                not get_only_non_empty or len(attr) > 0):
            result.append(
                (attr_name, len(attr), asizeof.asizeof(attr, detail=1)))
    return result


def get_memory_usage(obj, get_collections_memory_usage=False,
                     get_only_non_empty=False):
    result = []
    from pympler import asizeof
    result.append(asizeof.asizeof(obj))
    if get_collections_memory_usage:
        result.append(
            get_collection_sizes(
                obj, get_only_non_empty=get_only_non_empty))
    return result
