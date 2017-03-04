from collections import OrderedDict

import pytest

from plenum.common.eventually import eventually
from plenum.common.looper import Looper
from plenum.common.port_dispenser import genHa
from plenum.common.util import adict

from plenum.test.cli.helper import newCLI, checkAllNodesUp, loadPlugin, \
    doByCtx


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
def cli(cliLooper, tdir, tdirWithPoolTxns, tdirWithDomainTxns,
        tdirWithNodeKeepInited):
    return newCLI(cliLooper, tdir)


@pytest.fixture("module")
def aliceCli(cliLooper, tdir, tdirWithPoolTxns, tdirWithDomainTxns,
        tdirWithNodeKeepInited):
    return newCLI(cliLooper, tdir, unique_name='alice')



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
