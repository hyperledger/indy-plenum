from collections import OrderedDict

from common.serializers.msgpack_serializer import MsgPackSerializer

serializer = MsgPackSerializer()


def check(value):
    assert value == serializer.deserialize(serializer.serialize(value))


def check_dict(dict):
    assert OrderedDict(sorted(dict.items())) == serializer.deserialize(serializer.serialize(dict))


def test_serialize_int():
    check(1)
    check(111324324324324)
    check(-1)
    check(-44445423453245)
    check(0)


def test_serialize_str():
    check('aaaa')
    check('abc def')
    check('a b c d e f')
    check('строка')


def test_serialize_list():
    check([1, 'a', 'b'])


def test_serialize_float():
    check(0.34)
    check(-9.005)
    check(1231231.121212123)
    check(-1231231.121212123)


def test_serialize_ordered_dict():
    check({1: 'a', 2: 'b', 3: 'c'})
    check({3: 'a', 1: 'b', 2: 'c'})
    check({'ccc': 22, 'dd': 11, 'a': 44})


def test_serialize_complex_dict():
    check(
        {'integer': 36, 'name': 'Foo', 'surname': 'Bar', 'float': 14.8639,
         'index': 1, 'index_start_at': 56, 'email': 'foo@bar.com',
         'fullname': 'Foo Bar', 'bool': False})
    check(
        {'latitude': 31.351883, 'longitude': -97.466179,
         'tags': ['foo', 'bar', 'baz', 'alice', 'bob',
                  'carol', 'dave']})
    check(
        {'name': 'Alice Bob', 'website': 'example.com', 'friends': [
            {
                'id': 0,
                'name': 'Dave'
            },
            {
                'id': 1,
                'name': 'Carol'
            },
            {
                'id': 2,
                'name': 'Dave'
            }]})
