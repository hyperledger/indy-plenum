# from typing import List
#
# from bitarray import bitarray
#
# from common.exceptions import LogicError
#
#
# class BitmaskHelper:
#
#     @staticmethod
#     def pack_discarded_mask(value):
#         discarded_mask = bitarray()
#         discarded_mask.pack(bytes.fromhex(value))
#         return discarded_mask
#
#     @staticmethod
#     def unpack_discarded_mask(discarded_mask):
#         return discarded_mask.unpack().hex()
#
#     @staticmethod
#     def get_valid_reqs(reqIdrs, mask: bitarray):
#         return [b for a, b in zip(mask.tolist(), reqIdrs) if not a]
#
#     @staticmethod
#     def get_invalid_reqs(reqIdrs, mask: bitarray):
#         return [b for a, b in zip(mask.tolist(), reqIdrs) if a]
#
#     @staticmethod
#     def get_valid_count(mask: bitarray, len_reqIdr=0):
#         if mask.length() == 0:
#             return len_reqIdr
#         return mask.count(0)
#
#     @staticmethod
#     def get_invalid_count(mask: bitarray):
#         return mask.count(1)
#
#     @staticmethod
#     def drop_discarded_mask():
#         return bitarray()
#
#     @staticmethod
#     def apply_bitmask_to_list(reqIdrs: List, mask: bitarray):
#         if mask.length() == 0:
#             return reqIdrs, []
#         if len(reqIdrs) != mask.length():
#             raise LogicError("Length of reqIdr list and bitmask is not the same")
#         return BitmaskHelper.get_valid_reqs(reqIdrs, mask), BitmaskHelper.get_invalid_reqs(reqIdrs, mask)
