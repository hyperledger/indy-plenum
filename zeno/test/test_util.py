import math
from itertools import combinations

from libnacl import crypto_hash_sha256

from zeno.common.util import evenCompare, distributedConnectionMap, randomString
from zeno.test.greek import genNodeNames


def test_even_compare():
    vals = genNodeNames(24)
    for v1 in vals:
        beats = [v2 for v2 in vals if v1 != v2 and evenCompare(v1, v2)]
        print("{} beats {} others: {}".format(v1, len(beats), beats))
    evenCompare('Zeta', 'Alpha')

    def hashit(s):
        b = s.encode('utf-8')
        c = crypto_hash_sha256(b)
        return c.hex()

    for v in vals:
        print("{}: {}".format(v, hashit(v)))

    # TODO it appears this algorithm gives us something that is no better than alphabetical.
    # TODO we need to have an algorithm that has every node connecting to approximately half of the nodes.


def test_distributedConnectionMap():
    for nodeCount in range(2, 25):
        print("testing for node count: {}".format(nodeCount))
        names = genNodeNames(nodeCount)
        conmap = distributedConnectionMap(names)

        total_combinations = len(list(combinations(names, 2)))
        total_combinations_in_map = sum(len(x) for x in conmap.values())
        assert total_combinations_in_map == total_combinations

        maxPer = math.ceil(total_combinations / nodeCount)
        minPer = math.floor(total_combinations / nodeCount)
        for x in conmap.values():
            assert len(x) <= maxPer
            assert len(x) >= minPer


def test_distributedConnectionMapIsDeterministic():
    """
    While this doesn't prove determinism, it gives us confidence.
    For 20 iterations, it generates 24 random names, and 10 conmaps for those
    names, and compares that the conmaps generated for the same names are the
    same.
    """
    for _ in range(20):
        rands = [randomString() for _ in range(24)]
        conmaps = [distributedConnectionMap(rands) for _ in range(10)]
        for conmap1, conmap2 in combinations(conmaps, 2):
            assert conmap1 == conmap2
