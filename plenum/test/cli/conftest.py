import os
from collections import OrderedDict

import pytest

import plenum.common.util
from plenum.common.looper import Looper
from plenum.test.cli.mock_output import MockOutput
from plenum.test.eventually import eventually
from plenum.test.helper import genHa, TestNode, TestClient, checkNodesConnected
from plenum.common.util import adict

plenum.common.util.loggingConfigured = False

from plenum.test.cli.helper import TestCli


@pytest.yield_fixture(scope="module")
def looper():
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


def newCli(nodeRegsForCLI, looper, tdir):
    mockOutput = MockOutput()

    Cli = TestCli(looper=looper,
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
def cli(nodeRegsForCLI, looper, tdir):
    return newCli(nodeRegsForCLI, looper, tdir)


@pytest.fixture("module")
def validNodeNames(cli):
    return list(cli.nodeReg.keys())


@pytest.fixture("module")
def createAllNodes(cli):
    cli.enterCmd("new node all")

    def chk():
        msgs = {stmt['msg'] for stmt in cli.printeds}
        for nm in cli.nodes.keys():
            assert "{}:0 selected primary {} for instance 0 (view 0)"\
                .format(nm, cli.nodes[nm].replicas[0].primaryNames[0]) in msgs
            assert "{}:1 selected primary {} for instance 1 (view 0)"\
                .format(nm, cli.nodes[nm].replicas[1].primaryNames[0]) in msgs

    cli.looper.run(eventually(chk, retryWait=1, timeout=10))


@pytest.fixture("module")
def loadOpVerificationPlugin(cli):
    curPath = os.path.dirname(os.path.dirname(__file__))
    fullPath = os.path.join(curPath, 'plugin', 'plugin1')
    cli.enterCmd("load plugins from {}".format(fullPath))
    cli.looper.runFor(2)
