from weakref import WeakKeyDictionary

_lazy_value_cache = WeakKeyDictionary()


def lazy_field(prop):
    """
    Decorator which helps in creating lazy properties
    """
    @property
    def wrapper(self):
        if self not in _lazy_value_cache:
            _lazy_value_cache[self] = {}
        self_cache = _lazy_value_cache[self]
        if prop in self_cache:
            return self_cache[prop]
        prop_value = prop(self)
        self_cache[prop] = prop_value
        return prop_value
    return wrapper
