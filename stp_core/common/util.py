# TODO: move it to plenum-util repo
import inspect


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(
                Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def lxor(a, b):
    # Logical xor of 2 items, return true when one of them is truthy and
    # one of them falsy
    return bool(a) != bool(b)


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


def get_func_name(f):
    if hasattr(f, "__name__"):
        return f.__name__
    elif hasattr(f, "func"):
        return "partial({})".format(get_func_name(f.func))
    else:
        return "<unknown>"


def get_func_args(f):
    if hasattr(f, 'args'):
        return f.args
    else:
        return list(inspect.signature(f).parameters)
