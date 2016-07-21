import os
from collections import OrderedDict

import pytest

import plenum.common.util
from plenum.common.looper import Looper
from plenum.common.util import adict
from plenum.test.eventually import eventually
from plenum.test.helper import genHa

plenum.common.util.loggingConfigured = False

from plenum.test.cli.helper import newCLI, checkAllNodesUp


@pytest.yield_fixture(scope="module")
def looper():
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
def cli(nodeRegsForCLI, looper, tdir):
    return newCLI(nodeRegsForCLI, looper, tdir)


@pytest.fixture("module")
def validNodeNames(cli):
    return list(cli.nodeReg.keys())


@pytest.fixture("module")
def createAllNodes(cli):
    cli.enterCmd("new node all")
    cli.looper.run(eventually(checkAllNodesUp, cli, retryWait=1, timeout=20))


@pytest.fixture("module")
def loadOpVerificationPlugin(cli):
    curPath = os.path.dirname(os.path.dirname(__file__))
    fullPath = os.path.join(curPath, 'plugin', 'name_age_verification')
    cli.enterCmd("load plugins from {}".format(fullPath))
    cli.looper.runFor(2)
