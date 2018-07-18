from collections import OrderedDict

import pytest

from common.serializers.serialization import serialize_msg_for_signing


def test_serialize_int():
    assert b"1" == serialize_msg_for_signing(1)


def test_serialize_str():
    assert b"aaa" == serialize_msg_for_signing("aaa")


def test_serialize_none():
    assert b"" == serialize_msg_for_signing(None)


def test_serialize_simple_dict():
    assert b"1:a|2:b" == serialize_msg_for_signing({1: 'a', 2: 'b'})
    assert b"1:a|2:b" == serialize_msg_for_signing({"2": 'b', "1": 'a'})


def test_serialize_array():
    assert b"1,5,3,4,2" == serialize_msg_for_signing([1, 5, 3, 4, 2])


def test_serialize_ordered_dict():
    v1 = OrderedDict([('1', 'a'), ('2', 'b')])
    v2 = OrderedDict([('2', 'b'), ('1', 'a')])
    assert b"1:a|2:b" == serialize_msg_for_signing(v1)
    assert b"1:a|2:b" == serialize_msg_for_signing(v2)


def test_serialize_dict_with_array():
    assert b"1:a|2:b|3:1,2:k" == serialize_msg_for_signing({1: 'a', 2: 'b', 3: [1, {2: 'k'}]})
    assert b"1:a|2:b|3:1,2:k" == serialize_msg_for_signing({'1': 'a', '2': 'b', '3': ['1', {'2': 'k'}]})


@pytest.mark.skip("An issue in Signing Serializer: https://jira.hyperledger.org/browse/INDY-1469")
def test_serialize_dicts_with_different_keys():
    v1 = serialize_msg_for_signing(
        {
            1: 'a',
            2: {
                3: 'b',
                4: {
                    5: {
                        6: 'c'
                    }
                }
            }
        })
    v2 = serialize_msg_for_signing(
        {
            1: 'a',
            2: {
                3: 'b',
            },
            4: {
                5: {
                    6: 'c'
                }
            }
        })
    assert v1 == v2


@pytest.mark.skip("An issue in Signing Serializer: https://jira.hyperledger.org/browse/INDY-1469")
def test_serialize_complex_dict():
    assert b'1:a|2:3:b|2:4:5:6:c' == serialize_msg_for_signing(
        {
            1: 'a',
            2: {
                3: 'b',
                4: {
                    5: {
                        6: 'c'
                    }
                }
            }
        })
    assert b'1:a|2:3:b|2:4:5:6:c' == serialize_msg_for_signing(
        {
            '1': 'a',
            '2': {
                '3': 'b',
                '4': {
                    '5': {
                        '6': 'c'
                    }
                }
            }
        })
    v = serialize_msg_for_signing(
        {
            '1': 'a',
            '2': 'b',
            '3': {
                '4': 'c',
                '5': 'd',
                '6': {
                    '7': {
                        '8': 'e',
                        '9': 'f'
                    },
                    '10': {
                        '11': 'g',
                        '12': 'h'
                    }
                },
                '13': {
                    '13': {
                        '13': 'i',
                    }
                }
            }
        })
    assert b'1:a|2:b|3:4:c|3:5:d|3:6:7:8:e|3:6:7:9:f|3:6:10:11:g|3:6:10:12:h|3:13:13:13:i' == v


@pytest.mark.skip("An issue in Signing Serializer: https://jira.hyperledger.org/browse/INDY-1469")
def test_serialize_complex_ordered_dict():
    assert b'1:a|2:3:b|4:c' == serialize_msg_for_signing(
        OrderedDict([
            ('1', 'a'),
            ('2', OrderedDict([
                ('3', 'b'),
                ('4', 'c'),
            ]))
        ]))
    assert b'1:a|2:3:b|4:c' == serialize_msg_for_signing(
        OrderedDict([
            ('2', OrderedDict([
                ('4', 'c'),
                ('3', 'b'),
            ])),
            ('1', 'a'),
        ]))
