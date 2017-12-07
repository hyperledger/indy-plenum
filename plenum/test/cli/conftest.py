from collections import OrderedDict
from itertools import groupby

import pytest
from _pytest.recwarn import WarningsRecorder

from stp_core.loop.eventually import eventually
from stp_core.loop.looper import Looper
from stp_core.common.util import adict
from plenum.test.cli.helper import newCLI, waitAllNodesUp, loadPlugin, \
    doByCtx
from stp_core.network.port_dispenser import genHa


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
def cli(tdir, cliLooper, tdirWithPoolTxns, tdirWithDomainTxns,
        tdirWithClientPoolTxns, tdirWithNodeKeepInited):
    cli = newCLI(cliLooper, tdirWithClientPoolTxns, tdirWithPoolTxns, nodes_chroot=tdir)
    yield cli
    cli.close()


@pytest.fixture("module")
def aliceCli(tdir, cliLooper, tdirWithPoolTxns, tdirWithDomainTxns,
             tdirWithClientPoolTxns, tdirWithNodeKeepInited):
    cli = newCLI(cliLooper, tdirWithClientPoolTxns, tdirWithPoolTxns, nodes_chroot=tdir,
                 unique_name='alice')
    yield cli
    cli.close()


@pytest.fixture("module")
def validNodeNames(cli):
    return list(cli.nodeReg.keys())


@pytest.fixture("module")
def createAllNodes(request, cli):
    cli.enterCmd("new node all")
    waitAllNodesUp(cli)

    def stopNodes():
        for node in cli.nodes.values():
            node.stop()
    request.addfinalizer(stopNodes)


@pytest.fixture("module")
def loadOpVerificationPlugin(cli):
    loadPlugin(cli, 'name_age_verification')


@pytest.fixture("module")
def ctx():
    """
    Provides a simple container for test context. Assists with 'be' and 'do'.
    """
    return {}


@pytest.fixture("module")
def be(ctx):
    """
    Fixture that is a 'be' function that closes over the test context.
    'be' allows to change the current cli in the context.
    """
    def _(cli):
        ctx['current_cli'] = cli
    return _


@pytest.fixture("module")
def do(ctx):
    """
    Fixture that is a 'do' function that closes over the test context
    'do' allows to call the do method of the current cli from the context.
    """

    return doByCtx(ctx)
