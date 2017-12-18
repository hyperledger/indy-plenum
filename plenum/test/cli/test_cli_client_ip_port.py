import os

import pytest

from plenum.test.cli.helper import newCLI, newKeyPair
from plenum.test.helper import initDirWithGenesisTxns


@pytest.fixture(scope="module")
def cli1(cliLooper, tdirWithClientPoolTxns,
         tdirWithNodeKeepInited, tconf):
    tempDir = os.path.join(tdirWithClientPoolTxns, "cl1")
    initDirWithGenesisTxns(
        tempDir, tconf, tdirWithClientPoolTxns)
    cli = newCLI(cliLooper, tempDir, tempDir)
    yield cli
    cli.close()


@pytest.fixture(scope="module")
def cli2(cliLooper, tdir, tdirWithClientPoolTxns,
         tdirWithNodeKeepInited, tconf):
    tempDir = os.path.join(tdirWithClientPoolTxns, "cl2")
    initDirWithGenesisTxns(
        tempDir, tconf, tdirWithClientPoolTxns)
    cli = newCLI(cliLooper, tempDir, tempDir)
    yield cli
    cli.close()


def testEachClientOnDifferentPort(cli1, cli2):
    c1, c2 = "client1", "client2"
    newKeyPair(cli1)
    newKeyPair(cli2)
    cli1.enterCmd("new client {}".format(c1))
    cli2.enterCmd("new client {}".format(c2))
    client1 = next(iter(cli1.clients.values()))
    client2 = next(iter(cli2.clients.values()))
    assert client1.nodestack.ha != client2.nodestack.ha


def testClientListeningIp(cli, validNodeNames, createAllNodes):
    """
    Test that a client should always listen at 0.0.0.0
    """
    cName = "Joe"

    cli.enterCmd("new client {}".format(cName))
    assert cli.clients[cName].nodestack.ha[0] == '0.0.0.0'
