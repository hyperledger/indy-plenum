import random

from plenum.common.tools import lazy_field
from plenum.common.tools import _lazy_value_cache


class TestClass:
    @lazy_field
    def some_lazy(self):
        return random.random()

    @property
    def some_simple(self):
        return random.random()


def test_lazy_field():
    a = TestClass()
    b = TestClass()
    assert a.some_lazy == a.some_lazy
    assert b.some_lazy == b.some_lazy
    assert a.some_lazy != b.some_lazy
    assert a.some_simple != a.some_lazy
    assert a.some_simple != a.some_simple
    assert a.some_simple != b.some_simple


def check_weak_dict_is_used():
    a = TestClass()
    assert len(_lazy_value_cache.items()) == 0
    assert a.some_lazy == a.some_lazy
    assert len(_lazy_value_cache.items()) == 1
    del a
    assert len(_lazy_value_cache.items()) == 0
