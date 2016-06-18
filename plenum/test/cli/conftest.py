import os
from collections import OrderedDict

import pytest

import plenum.common.util
from plenum.common.looper import Looper
from plenum.test.cli.mock_output import MockOutput
from plenum.test.helper import genHa, TestNode, TestClient
from plenum.common.util import adict

plenum.common.util.loggingConfigured = False

from plenum.test.cli.helper import TestCli


@pytest.yield_fixture(scope="module")
def cliLooper():
    with Looper(debug=False) as l:
        yield l


@pytest.fixture("module")
def nodeRegsForCLI():
    nodeNames = ['Alpha', 'Beta', 'Gamma', 'Delta']
    has = [genHa(2) for _ in nodeNames]
    nodeNamesC = [n + 'C' for n in nodeNames]
    nodeReg = OrderedDict((n, has[i][0]) for i, n in enumerate(nodeNames))
    cliNodeReg = OrderedDict((n, has[i][1]) for i, n in enumerate(nodeNamesC))
    return adict(nodeReg=nodeReg, cliNodeReg=cliNodeReg)


@pytest.fixture("module")
def cli(nodeRegsForCLI, cliLooper, tdir):
    mockOutput = MockOutput()

    Cli = TestCli(looper=cliLooper,
                  basedirpath=tdir,
                  nodeReg=nodeRegsForCLI.nodeReg,
                  cliNodeReg=nodeRegsForCLI.cliNodeReg,
                  output=mockOutput,
                  debug=True)
    Cli.NodeClass = TestNode
    Cli.ClientClass = TestClient
    Cli.basedirpath = tdir
    return Cli


@pytest.fixture("module")
def validNodeNames(cli):
    return list(cli.nodeReg.keys())


@pytest.fixture("module")
def createAllNodes(cli):
    cli.enterCmd("new node all")
    cli.looper.runFor(5)


@pytest.fixture("module")
def allNodesUp(cli, createAllNodes, up):
    # Let nodes complete election and the output be rendered on the screen
    cli.looper.runFor(5)


@pytest.fixture("module")
def loadOpVerificationPlugin(cli):
    curPath = os.path.dirname(os.path.dirname(__file__))
    fullPath = os.path.join(curPath, 'plugin', 'plugin1')
    cli.enterCmd("load plugins from {}".format(fullPath))
    cli.looper.runFor(2)
