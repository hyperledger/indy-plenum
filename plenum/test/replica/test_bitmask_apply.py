# import pytest
# from bitarray import bitarray
#
# from common.exceptions import LogicError
# from plenum.common.bitmask_helper import BitmaskHelper
#
#
# def test_apply_bitmask(replica):
#     """Expected result : [1, 3] [2] (the first returned parameter is a list of valid requests
#                                      and the second is a list of invalid reqs)
#        bitmask is 101
#     """
#     filtered_list = [1, 2, 3]
#     mask = bitarray()
#     mask.append(False)
#     mask.append(True)
#     mask.append(False)
#     valid, invalid = BitmaskHelper.apply_bitmask_to_list(filtered_list, mask)
#     assert [1, 3] == valid
#     assert [2] == invalid
#
#
# def test_raise_logicError():
#     """Incoming list and bitmask have different length"""
#     filtered_list = [1, 2, 3, 4]
#     mask = bitarray()
#     mask.append(False)
#     mask.append(True)
#     mask.append(False)
#     with pytest.raises(LogicError) as e:
#         BitmaskHelper.apply_bitmask_to_list(filtered_list, mask)
#
#
# def test_get_valid_reqs():
#     """Getting list of valid reqs (1 in bitmask)"""
#     filtered_list = [1, 2, 3]
#     mask = bitarray()
#     mask.append(False)
#     mask.append(True)
#     mask.append(False)
#     assert [1, 3] == BitmaskHelper.get_valid_reqs(filtered_list, mask)
#
# def test_get_invalid_reqs():
#     """Getting list of invalid reqs (0 in bitmask)"""
#     filtered_list = [1, 2, 3]
#     mask = bitarray()
#     mask.append(False)
#     mask.append(True)
#     mask.append(False)
#     assert [2] == BitmaskHelper.get_invalid_reqs(filtered_list, mask)
#
#
# def test_get_valid_count_reqs():
#     """Getting count of valid reqs in bitmask"""
#     mask = bitarray()
#     mask.append(False)
#     mask.append(True)
#     mask.append(False)
#     assert 2 == BitmaskHelper.get_valid_count(mask)
#
#
# def test_get_invalid_count_reqs():
#     """Getting count of valid reqs in bitmask"""
#     mask = bitarray()
#     mask.append(False)
#     mask.append(True)
#     mask.append(False)
#     assert 1 == BitmaskHelper.get_invalid_count(mask)
#
#
# def test_pack_unpack():
#     mask = bitarray()
#     for i in range(1000):
#         mask.append(True if i % 2 else False)
#     unpacked = BitmaskHelper.unpack_discarded_mask(mask)
#     packed = BitmaskHelper.pack_discarded_mask(unpacked)
#     assert mask.tostring() == packed.tostring()
