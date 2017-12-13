import pytest

from plenum.common.util import firstValue
from plenum.test.cli.helper import newKeyPair


@pytest.fixture(scope="module")
def pubKey(cli):
    return newKeyPair(cli, "test")


def testKeyPair(cli, pubKey):
    pass


def testUseKeyPair(cli, pubKey):
    cli.enterCmd('use DID {}'.format("test"))
    assert cli.activeAlias == "test"


def testBecome(cli, pubKey):
    cli.enterCmd("become test")
    assert cli.activeAlias == "test"
