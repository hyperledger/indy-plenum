import os
from collections import OrderedDict

import pytest

import plenum.common.util
from plenum.common.looper import Looper
from plenum.common.util import adict
from plenum.test.eventually import eventually
from plenum.test.helper import genHa

plenum.common.util.loggingConfigured = False

from plenum.test.cli.helper import newCLI, checkAllNodesUp, loadPlugin


@pytest.yield_fixture(scope="module")
def cliLooper():
    with Looper(debug=False) as l:
        yield l


@pytest.fixture("module")
def nodeNames():
    return ['Alpha', 'Beta', 'Gamma', 'Delta']


@pytest.fixture("module")
def nodeRegsForCLI(nodeNames):
    nodeNames = ['Alpha', 'Beta', 'Gamma', 'Delta']
    has = [genHa(2) for _ in nodeNames]
    nodeNamesC = [n + 'C' for n in nodeNames]
    nodeReg = OrderedDict((n, has[i][0]) for i, n in enumerate(nodeNames))
    cliNodeReg = OrderedDict((n, has[i][1]) for i, n in enumerate(nodeNamesC))
    return adict(nodeReg=nodeReg, cliNodeReg=cliNodeReg)


@pytest.fixture("module")
def cli(nodeRegsForCLI, cliLooper, tdir):
    return newCLI(nodeRegsForCLI, cliLooper, tdir)


@pytest.fixture("module")
def validNodeNames(cli):
    return list(cli.nodeReg.keys())


@pytest.fixture("module")
def createAllNodes(request, cli):
    cli.enterCmd("new node all")
    cli.looper.run(eventually(checkAllNodesUp, cli, retryWait=1, timeout=20))

    def stopNodes():
        for node in cli.nodes.values():
            node.stop()

    request.addfinalizer(stopNodes)


@pytest.fixture("module")
def loadOpVerificationPlugin(cli):
    loadPlugin(cli, 'name_age_verification')

