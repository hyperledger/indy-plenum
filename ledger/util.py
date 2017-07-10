from collections import namedtuple
from enum import Enum


def count_bits_set(i):
    # from https://wiki.python.org/moin/BitManipulation
    count = 0
    while i:
        i &= i - 1
        count += 1
    return count


def isPowerOf2(i):
    return count_bits_set(i) == 1


def lowest_bit_set(i):
    # from https://wiki.python.org/moin/BitManipulation
    # but with 1-based indexing like in ffs(3) POSIX
    return highest_bit_set(i & -i)


def highest_bit_set(i):
    # from https://wiki.python.org/moin/BitManipulation
    # but with 1-based indexing like in ffs(3) POSIX
    hi = i
    hiBit = 0
    while hi:
        hi >>= 1
        hiBit += 1
    return hiBit


def highestPowerOf2LessThan(n):
    return n.bit_length() - 1


class F(Enum):
    clientId = 1
    requestId = 2
    rootHash = 3
    created = 4
    addedToTree = 5
    auditPath = 6
    seqNo = 7
    treeSize = 8
    leafHash = 9
    nodeHash = 10
    height = 11


STH = namedtuple("STH", ["tree_size", "sha256_root_hash"])

class ConsistencyVerificationFailed(Exception):
    pass
