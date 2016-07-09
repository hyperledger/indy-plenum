import pytest

from plenum.test.cli.helper import newKeyPair


@pytest.fixture(scope="module")
def pubKey(cli):
    return newKeyPair(cli, "test")


def testKeyPair(cli, pubKey):
    # output = cli.lastCmdOutput
    # TODO check the output
    """
    Key created in wallet Joseph
    Identifier for key is <cryptonym>
    Current identifier set to <cryptonym>
    """
    pass


def testUseKeyPair(cli, pubKey):
    cli.enterCmd('use keypair {}'.format(pubKey))
    assert cli.activeSigner.verstr == pubKey


def testBecome(cli, pubKey):
    cli.enterCmd("become test")
    assert cli.activeSigner.verstr == pubKey
