import pytest

from plenum.common.looper import Looper
from plenum.common.util import firstValue
from plenum.test.cli.helper import newCLI


def printDefaultClientAndIdentifier(cli):
    dc = cli.defaultClient
    verstr = firstValue(dc.signers).verstr
    assert cli.printeds[1]['msg'] == "Current wallet set to {walletName}". \
        format(walletName=dc.name)
    assert cli.printeds[0]['msg'] == \
           "Current identifier set to {alias} ({cryptonym})". \
               format(alias=dc.name, cryptonym=verstr)


@pytest.fixture(scope="module")
def initStatements(cli):
    name = cli.defaultClient.name
    cryptonym = firstValue(cli.defaultClient.signers).verstr
    return ["New wallet {} created".format(name),
            "Current wallet set to " + name,
            "Key created in wallet " + name,
            "Identifier for key is " + cryptonym,
            "Current identifier set to " + cryptonym,
            "Note: To rename this wallet, use following command:",
            "    rename wallet Default to NewName"]


def testFirstStartup(cli, initStatements):
    messages = printedMessages(cli)
    assert set(initStatements).issubset(messages)
    printDefaultClientAndIdentifier(cli)


@pytest.yield_fixture(scope="module")
def newLooper():
    with Looper(debug=False) as l:
        yield l


@pytest.fixture("module")
def reincarnatedCLI(nodeRegsForCLI, newLooper, tdir, cli):
    """
    Creating a new cli instance is equivalent to starting and stopping a cli
    """
    return newCLI(nodeRegsForCLI, newLooper, tdir)


def testSubsequentStartup(reincarnatedCLI, initStatements):
    messages = printedMessages(reincarnatedCLI)
    assert not set(initStatements).issubset(messages)
    printDefaultClientAndIdentifier(reincarnatedCLI)


def printedMessages(cli):
    return set([p['msg'] for p in cli.printeds])
