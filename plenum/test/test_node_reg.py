from unittest import TestCase

from plenum.test.test_node import genNodeReg


class TestNodeReg(TestCase):
    def test_genNodeReg(self):
        l = 10
        nr = genNodeReg(count=l)
        cli = nr.extractCliNodeReg()

        # lengths are correct
        assert len(cli) == l
        assert len(nr) == l

        # node names don't overlap
        assert set(nr.keys()) & set(cli.keys()) == set()

        # checking port values
        nrports = [x.ha.port for x in nr.values()]
        cliports = [x.port for x in cli.values()]

        # ports are all numbers
        allports = set()
        for s in [nrports, cliports]:
            for p in s:
                assert isinstance(p, int)
                allports.add(p)

        # ports don't overlap
        assert len(allports) == l * 2
