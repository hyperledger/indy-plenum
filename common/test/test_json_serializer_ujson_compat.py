"""
Regression tests added when the ``ujson`` dependency was removed: pin the
exact byte output of ``JsonSerializer`` (sorted keys, compact separators,
raw UTF-8), since it feeds signing/hashing and must stay byte-stable.
"""
import pytest

from common.serializers.json_serializer import JsonSerializer


@pytest.fixture
def sz():
    return JsonSerializer()


def test_keys_sorted_and_compact(sz):
    # Insertion order must not affect output; keys are sorted, no whitespace.
    assert sz.serialize({'b': 2, 'a': 1, 'c': 3}, toBytes=False) == '{"a":1,"b":2,"c":3}'


def test_non_ascii_kept_raw_utf8(sz):
    # ensure_ascii=False: characters are emitted as raw UTF-8, not \\uXXXX.
    assert sz.serialize({'name': 'héllo', 'kr': '한글', 'emoji': '🚀'},
                        toBytes=False) == '{"emoji":"🚀","kr":"한글","name":"héllo"}'
    # And the bytes form is the UTF-8 encoding of that string.
    assert sz.serialize({'name': 'héllo'}) == '{"name":"héllo"}'.encode('utf-8')


def test_float_repr_pinned(sz):
    assert sz.serialize({'a': 14.8639, 'b': -97.466179, 'c': 1.0, 'd': 1e20},
                        toBytes=False) == '{"a":14.8639,"b":-97.466179,"c":1.0,"d":1e+20}'


def test_int_keys_become_strings(sz):
    # json coerces non-string keys to strings and then sorts lexicographically.
    assert sz.serialize({3: 'c', 1: 'a', 2: 'b'}, toBytes=False) == '{"1":"a","2":"b","3":"c"}'


def test_bool_and_none(sz):
    assert sz.serialize({'t': True, 'f': False, 'n': None},
                        toBytes=False) == '{"f":false,"n":null,"t":true}'


def test_empty_dict(sz):
    assert sz.serialize({}, toBytes=False) == '{}'


def test_top_level_bytes_base64(sz):
    # The OrderedJsonEncoder.encode override base64-encodes a top-level
    # bytes/bytearray value (b'raw' -> base64 'cmF3').
    assert sz.serialize(b'raw', toBytes=False) == '"cmF3"'
    assert sz.serialize(bytearray(b'raw'), toBytes=False) == '"cmF3"'


def test_round_trip(sz):
    data = {'name': 'Alice', 'n': 1, 'f': 1.5, 'b': True, 'list': [1, 'two', None]}
    assert sz.deserialize(sz.serialize(data)) == data
    assert sz.deserialize(sz.serialize(data, toBytes=False)) == data


@pytest.mark.parametrize('value', [
    {'z': b'raw'},      # bytes nested in a dict value
    [b'raw'],           # bytes nested in a list
    {'z': bytearray(b'raw')},
])
def test_nested_bytes_raise_typeerror(sz, value):
    """
    The bytes special-case in ``OrderedJsonEncoder.encode`` only fires for a
    top-level value; bytes nested in a container raise TypeError. A fix, if
    ever needed, belongs in ``OrderedJsonEncoder.default``.
    """
    with pytest.raises(TypeError):
        sz.serialize(value)
