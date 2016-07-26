import os

import pytest
from plenum.test.cli.helper import createClientAndConnect, newCLI, newKeyPair


@pytest.fixture(scope="module")
def cli1(nodeRegsForCLI, looper, tdir):
    tempDir = os.path.join(tdir, "cl1")
    return newCLI(nodeRegsForCLI, looper, tempDir)


@pytest.fixture(scope="module")
def cli2(nodeRegsForCLI, looper, tdir):
    tempDir = os.path.join(tdir, "cl2")
    return newCLI(nodeRegsForCLI, looper, tempDir)


def testEachClientOnDifferentPort(cli1, cli2):
    c1, c2 = "client1", "client2"
    newKeyPair(cli1)
    newKeyPair(cli2)
    cli1.enterCmd("new client {}".format(c1))
    cli2.enterCmd("new client {}".format(c2))
    client1 = next(iter(cli1.clients.values()))
    client2 = next(iter(cli2.clients.values()))
    assert client1.nodestack.ha != client2.nodestack.ha

