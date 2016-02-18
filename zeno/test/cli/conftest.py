import os
from configparser import ConfigParser

import pytest

import zeno.common.util
from zeno.common.looper import Looper
from zeno.test.helper import ensureElectionsDone

zeno.common.util.loggingConfigured = False

from zeno.test.cli.helper import TestCli


@pytest.yield_fixture(scope="module")
def cliLooper():
    with Looper(debug=False) as l:
        yield l


@pytest.fixture("module")
def cli(cliLooper, tdir):
    cfg = ConfigParser()
    cfgPath = os.path.abspath(os.path.join(os.path.abspath(os.path.dirname(
        __file__)), '../../../scripts/node_reg.conf'))
    cfg.read(cfgPath)

    nodeReg = TestCli.loadNodeReg(cfg)
    cliNodeReg = TestCli.loadCliNodeReg(cfg)
    Cli = TestCli(looper=cliLooper,
                  tmpdir=tdir,
                  nodeReg=nodeReg,
                  cliNodeReg=cliNodeReg,
                  debug=True)
    return Cli


@pytest.fixture("module")
def validNodeNames(cli):
    return list(cli.nodeReg.keys())


@pytest.fixture("module")
def createAllNodes(cli):
    cli.enterCmd("new node all")


@pytest.fixture("module")
def allNodesUp(cli, createAllNodes, cliLooper):
    # Let nodes complete election and the output be rendered on the screen
    cli.looper.runFor(5)
    ensureElectionsDone(looper=cliLooper,
                        nodes=cli.nodes.values(),
                        retryWait=1,
                        timeout=30)
