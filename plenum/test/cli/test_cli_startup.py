import pytest

from stp_core.loop.looper import Looper
from plenum.common.util import firstValue
from plenum.test.cli.helper import newCLI


def assertPrintsDefaultClientAndIdentifier(cli):
    dc = cli.activeClient
    verstr = firstValue(dc.signers).verstr
    assert cli.printeds[1]['msg'] == "Current wallet set to {walletName}". \
        format(walletName=dc.name)
    assert cli.printeds[0]['msg'] == \
        "Current identifier set to {alias} ({cryptonym})". \
        format(alias=dc.name, cryptonym=verstr)


def printedMessages(cli):
    return set([p['msg'] for p in cli.printeds])


@pytest.fixture(scope="module")
def initStatements(cli):
    name = cli.activeClient.name
    cryptonym = firstValue(cli.activeClient.signers).verstr
    return ["New wallet {} created".format(name),
            "Current wallet set to " + name,
            "Key created in wallet " + name,
            "DID for key is " + cryptonym,
            "Current identifier set to " + cryptonym,
            "Note: To rename this wallet, use following command:",
            "    rename wallet Default to NewName"]


@pytest.yield_fixture(scope="module")
def newLooper():
    with Looper(debug=False) as l:
        yield l


@pytest.fixture("module")
def reincarnatedCLI(nodeRegsForCLI, newLooper, tdir, cli):
    """
    Creating a new cli instance is equivalent to starting and stopping a cli
    """
    cli = newCLI(nodeRegsForCLI, newLooper, tdir, unique_name='reincarnate')
    yield cli
    cli.close()


@pytest.mark.skip(reason="SOV-542. Implementation changed")
def testFirstStartup(cli, initStatements):
    messages = printedMessages(cli)
    assert set(initStatements).issubset(messages)
    assertPrintsDefaultClientAndIdentifier(cli)


@pytest.mark.skip(reason="SOV-543. Implementation changed")
def testSubsequentStartup(reincarnatedCLI, initStatements):
    messages = printedMessages(reincarnatedCLI)
    assert not set(initStatements).issubset(messages)
    assertPrintsDefaultClientAndIdentifier(reincarnatedCLI)
