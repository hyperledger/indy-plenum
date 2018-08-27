import pytest
from bitarray import bitarray

from common.exceptions import LogicError


def test_apply_bitmask(replica):
    """Expected result : [1, 3] [2] (the first returned parameter is list of valid requests
                                     and the second is list of invalid reqs)
       bitmask is 101
    """
    filtered_list = [1, 2, 3]
    mask = bitarray()
    mask.append(True)
    mask.append(False)
    mask.append(True)
    valid, invalid = replica._apply_bitmask_to_list(filtered_list, mask)
    assert [1, 3] == valid
    assert [2] == invalid


def test_raise_logicError(replica):
    """Incoming list and bitmask have different length"""
    filtered_list = [1, 2, 3, 4]
    mask = bitarray()
    mask.append(True)
    mask.append(False)
    mask.append(True)
    with pytest.raises(LogicError) as e:
        replica._apply_bitmask_to_list(filtered_list, mask)


def test_get_valid_reqs(replica):
    """Getting list of valid reqs (1 in bitmask)"""
    filtered_list = [1, 2, 3]
    mask = bitarray()
    mask.append(True)
    mask.append(False)
    mask.append(True)
    assert [1, 3] == replica._get_valid_reqs(filtered_list, mask)

def test_get_invalid_reqs(replica):
    """Getting list of invalid reqs (0 in bitmask)"""
    filtered_list = [1, 2, 3]
    mask = bitarray()
    mask.append(True)
    mask.append(False)
    mask.append(True)
    assert [2] == replica._get_invalid_reqs(filtered_list, mask)