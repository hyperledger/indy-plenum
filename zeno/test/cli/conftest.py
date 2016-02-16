import pytest

import zeno.common.util
from zeno.test.helper import checkPoolReady

zeno.common.util.loggingConfigured = False

from zeno.cli.__main__ import main
from zeno.test.cli.helper import TestCli


@pytest.fixture("module")
def cli(looper):
    Cli = main(isTesting=True, cliClass=TestCli)
    # A new cli should have no nodes
    assert Cli.nodes == {}
    Cli.looper = looper
    return Cli


@pytest.fixture("module")
def validNodeNames(cli):
    return list(cli.nodeReg.keys())


@pytest.fixture("module")
def createAllNodes(cli):
    cli.enterCmd("new node all")


@pytest.fixture("module")
def allNodesUp(cli, createAllNodes, up):
    pass