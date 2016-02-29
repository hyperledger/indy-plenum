import os
from collections import OrderedDict
from configparser import ConfigParser

import pytest

import zeno.common.util
from zeno.common.looper import Looper
from zeno.test.helper import ensureElectionsDone, genHa
from zeno.test.testing_utils import adict

zeno.common.util.loggingConfigured = False

from zeno.test.cli.helper import TestCli


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
    return adict(nodeReg=nodeReg, cliNodeReg = cliNodeReg)


@pytest.fixture("module")
def cli(nodeRegsForCLI, cliLooper, tdir):
    Cli = TestCli(looper=cliLooper,
                  tmpdir=tdir,
                  nodeReg=nodeRegsForCLI.nodeReg,
                  cliNodeReg=nodeRegsForCLI.cliNodeReg,
                  debug=True)
    return Cli


@pytest.fixture("module")
def validNodeNames(cli):
    return list(cli.nodeReg.keys())


@pytest.fixture("module")
def createAllNodes(cli):
    cli.enterCmd("new node all")


@pytest.fixture("module")
def allNodesUp(cli, createAllNodes, up):
    # Let nodes complete election and the output be rendered on the screen
    cli.looper.runFor(5)
