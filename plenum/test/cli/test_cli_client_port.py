import os
from shutil import copyfile

import pytest
from plenum.test.cli.helper import createClientAndConnect, newCLI, newKeyPair


def prepDir(dirName, tdirWithPoolTxns, tdirWithDomainTxns, tconf):
    os.makedirs(dirName)
    copyfile(os.path.join(tdirWithPoolTxns, tconf.poolTransactionsFile),
             os.path.join(dirName, tconf.poolTransactionsFile))
    copyfile(os.path.join(tdirWithDomainTxns, tconf.domainTransactionsFile),
             os.path.join(dirName, tconf.domainTransactionsFile))


@pytest.fixture(scope="module")
def cli1(cliLooper, tdir, tdirWithPoolTxns, tdirWithDomainTxns,
        tdirWithNodeKeepInited, tconf):
    tempDir = os.path.join(tdir, "cl1")
    prepDir(tempDir, tdirWithPoolTxns, tdirWithDomainTxns, tconf)
    return newCLI(cliLooper, tempDir)


@pytest.fixture(scope="module")
def cli2(cliLooper, tdir, tdirWithPoolTxns, tdirWithDomainTxns,
        tdirWithNodeKeepInited, tconf):
    tempDir = os.path.join(tdir, "cl2")
    prepDir(tempDir, tdirWithPoolTxns, tdirWithDomainTxns, tconf)
    return newCLI(cliLooper, tempDir)


def testEachClientOnDifferentPort(cli1, cli2):
    c1, c2 = "client1", "client2"
    newKeyPair(cli1)
    newKeyPair(cli2)
    cli1.enterCmd("new client {}".format(c1))
    cli2.enterCmd("new client {}".format(c2))
    client1 = next(iter(cli1.clients.values()))
    client2 = next(iter(cli2.clients.values()))
    assert client1.nodestack.ha != client2.nodestack.ha

