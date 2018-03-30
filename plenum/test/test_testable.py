import unittest
from typing import Any
from unittest import TestCase

from stp_core.common.log import getlogger
from plenum.server.node import Node
from plenum.test.testable import spyable

pr = slice(3, 5)  # params and result

logger = getlogger()


class TestHelpers(TestCase):
    def subTestV(self, msg=None, **params):  # verbose subtest
        if msg is not None:
            logger.info("-------Starting sub test: " + msg)
        return self.subTest(msg, **params)

    def checkOneInit(self, z, params):
        with self.subTest(
                "ensure __init__ entry is there for {}".format(params)):
            inits = z.spylog.getAll('__init__')
            self.assertEqual(len(inits), 1, "expected only 1 init entry")
            self.assertEqual(inits[0][pr], (params, None),
                             "expected proper init params and result")

    def runThroughAssertions(self, z, ovrdCornResult: str = None):
        ok = 'ok' if not ovrdCornResult else ovrdCornResult
        yucky = 'yuckxy' if not ovrdCornResult else ovrdCornResult
        with self.subTest("ensure init properly runs"):
            assert z.s == 'x'
            assert z.p == 'y'

        z.mymethod("hello")

        with self.subTest(
                "ensure first call of method #2 returns the proper params and result"):
            z.eatCorn('canned')
            self.assertEqual(z.spylog.getLast('eatCorn')[pr],
                             ({'kind': 'canned'}, yucky))
            assert z.spylog.count('eatCorn') == 1

        with self.subTest(
                "ensure second call of method #2 returns the proper params and result"):
            res = z.eatCorn('fresh')
            assert res == ok
            assert z.spylog.getLast('eatCorn')[pr] == ({'kind': 'fresh'}, ok)
            assert z.spylog.count('eatCorn') == 2

        with self.subTest(
                "ensure third call of method #2  returns the proper params and result"):
            z.eatCorn('canned')
            assert z.spylog.getLast('eatCorn')[pr] == (
                {'kind': 'canned'}, yucky)
            assert z.spylog.count('eatCorn') == 3

        self.checkOneInit(z, {'s': 'x', 'p': 'y'})

        with self.subTest("ensure entries accummulate properly"):
            z.eatCorn('canned')
            z.eatCorn('whirled')
            self.assertEqual(len(z.spylog), 7,
                             "expected 7 entries in the spy log")
            z.eatCorn('creamed')
            z.eatCorn('creamed')
            z.eatCorn('spun')
            z.mymethod("hello again")
            self.assertEqual(len(z.spylog), 11,
                             "expected 11 entries in the spy log")

        with self.subTest("exceptions are handled gracefully (logged, etc.)"):
            # TODO!
            pass


class NewTestableTests(TestHelpers):
    def testNew(self):
        X = spyable(Node)
        print(X)

    def testSpyableBaseClass(self):
        SpyBaseClass = spyable()(NewBaseClass)
        z = SpyBaseClass('x', 'y')
        self.runThroughAssertions(z)

    def testSpyableSubClass(self):
        SpySubClass = spyable()(SubClass)
        z = SpySubClass('x', 'y')
        self.runThroughAssertions(z)

    def testSpyableSubClassWithOverride(self):
        SpySubClassOvrd = spyable()(SubClassWithOverride)
        z = SpySubClassOvrd('x', 'y')
        self.runThroughAssertions(z, 'hooray!')

    def testEveryObjectGetsItsOwnSpyLog(self):
        SpySubClass = spyable()(SubClass)
        y = SpySubClass('a', 'b')
        z = SpySubClass('x', 'y')
        self.checkOneInit(y, {'s': 'a', 'p': 'b'})
        self.checkOneInit(z, {'s': 'x', 'p': 'y'})

    def testSpyOnSubsetOfMethods(self):
        def go(methods, ec: "expected counts"):
            SpySubClass = spyable(methods=methods)(SubClass)
            z = SpySubClass('a', 'b')
            self.assertEqual(len(z.spylog), ec[0],
                             "expected certain number of entries in the spy log")
            z.eatCorn('canned')
            z.eatCorn('whirled')
            self.assertEqual(len(z.spylog), ec[1],
                             "expected certain number of entries in the spy log")
            z.eatCorn('creamed')
            z.mymethod("hello again")
            self.assertEqual(len(z.spylog), ec[2],
                             "expected certain number of entries in the spy log")

        with TestHelpers.subTest(self, "No subset"):
            go(None, [1, 3, 5])
        with TestHelpers.subTest(self, "One method"):
            go(["eatCorn"], [0, 2, 3])
        with TestHelpers.subTest(self, "Two methods"):
            go(["eatCorn", "mymethod"], [0, 2, 4])
        with TestHelpers.subTest(self,
                                 "One method using function references instead of names"):
            go([SubClass.eatCorn], [0, 2, 3])
        with TestHelpers.subTest(self,
                                 "Two methods using function references instead of names"):
            go([SubClass.eatCorn, SubClass.mymethod], [0, 2, 4])

    def testSpyOnOverriddenClassMethod(self):
        SpySubClass = spyable(
            methods=[SubClassWithOverride.eatCorn, "mymethod"])(
            SubClassWithOverride)
        z = SpySubClass('a', 'b')
        z.mymethod("hi")
        z.eatCorn("canned")
        self.assertEqual(z.spylog.getLast('eatCorn')[pr],
                         ({'kind': 'canned'}, 'hooray!'))
        self.assertEqual(z.spylog.getLast('mymethod')[pr],
                         ({'inp': 'hi'}, None))

    def testSpyOnOverriddenBaseClassMethod(self):
        SpySubClass = spyable(methods=[NewBaseClass.eatCorn, "mymethod"])(
            SubClassWithOverride)
        z = SpySubClass('a', 'b')
        z.mymethod("hi")
        z.eatCorn("canned")
        self.assertEqual(z.spylog.getLast('eatCorn'), None)
        self.assertEqual(z.spylog.getLast('mymethod')[pr],
                         ({'inp': 'hi'}, None))

    def testSpyOnCertainClass(self):
        # known limitation... when super() is called, we are not spy-wrapping
        # base base class methods.
        SpySubClass = spyable(methods=[NewBaseClass.eatCorn, "mymethod"])(
            SubClassWithOverrideAndSuperCall)
        z = SpySubClass('a', 'b')
        z.mymethod("hi")
        z.eatCorn("canned")
        self.assertEqual(z.spylog.getLast('eatCorn'), None)
        self.assertEqual(z.spylog.getLast('mymethod')[pr], ({'inp': 'hi'},
                                                            None))


class NewBaseClass:
    def __init__(self, s, p):
        self.s = s
        self.p = p

    def mymethod(self, inp: Any) -> None:
        logger.debug(
            "input '{}' and values '{}', '{}'".format(inp, self.s, self.p))

    def eatCorn(self, kind: str) -> str:
        return 'yuck' + self.s + self.p if kind == 'canned' else 'ok'


class SubClass(NewBaseClass):
    pass


class SubClassWithOverride(NewBaseClass):
    # loves all kinds of corn
    def eatCorn(self, kind: str) -> str:
        return 'hooray!'


class SubClassWithOverrideAndSuperCall(NewBaseClass):
    def eatCorn(self, kind: str) -> str:
        return "overridden: " + super().eatCorn(kind)


class BaseClassNoMC:
    def __init__(self, s, p):
        self.s = s
        self.p = p

    def mymethod(self, inp: Any) -> None:
        logger.debug(
            "input '{}' and values '{}', '{}'".format(inp, self.s, self.p))

    def eatCorn(self, kind: str) -> str:
        return 'yuck' + self.s + self.p if kind == 'canned' else 'ok'


def runSome():
    """ Unittest runner """
    tests = []

    suite = unittest.TestSuite(tests)
    unittest.TextTestRunner(verbosity=2).run(suite)


def runAll():
    """ Unittest runner """
    suite = unittest.TestSuite()
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == '__main__' and __package__ is None:
    runAll()
