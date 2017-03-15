from random import shuffle, randint

import pytest
from ioflo.aid import getConsole

from plenum.common.constants import CLIENT_STACK_SUFFIX
from stp_core.loop.eventually import eventually
from plenum.common.log import getlogger
from stp_core.loop.looper import Looper
from plenum.common.temp_file_util import SafeTemporaryDirectory
from plenum.common.types import NodeDetail
from plenum.test import waits
from plenum.test.helper import stopNodes
from plenum.test.test_node import TestNode, checkNodesConnected, \
    checkProtocolInstanceSetup
from stp_zmq.test.helper import genKeys
from stp_core.network.port_dispenser import genHa

logger = getlogger()

whitelist = ['discarding message', 'found legacy entry',
             'error while verifying message']


@pytest.fixture()
def nodeReg():
    return {
            'Alpha': NodeDetail(genHa(1), "AlphaC", genHa(1)),
            'Beta': NodeDetail(genHa(1), "BetaC", genHa(1)),
            'Gamma': NodeDetail(genHa(1), "GammaC", genHa(1)),
            'Delta': NodeDetail(genHa(1), "DeltaC", genHa(1))
    }


# Its a function fixture, deliberately
@pytest.yield_fixture()
def tdirAndLooper(nodeReg):
    with SafeTemporaryDirectory() as td:
        logger.debug("temporary directory: {}".format(td))
        with Looper() as looper:
            yield td, looper


@pytest.mark.skip()
def testNodesConnectsWhenOneNodeIsLate(allPluginsPath, tdirAndLooper,
                                       nodeReg, conf):
    tdir, looper = tdirAndLooper
    nodes = []
    names = list(nodeReg.keys())
    logger.debug("Node names: {}".format(names))

    def create(name):
        node = TestNode(name, nodeReg, basedirpath=tdir,
                        pluginPaths=allPluginsPath)
        looper.add(node)
        nodes.append(node)

    # TODO: This will be moved to a fixture
    if conf.UseZStack:
        genKeys(tdir, names + [_+CLIENT_STACK_SUFFIX for _ in names])

    for name in names[:3]:
        create(name)

    looper.run(checkNodesConnected(nodes))

    # wait for the election to complete with the first three nodes
    looper.runFor(10)

    # create the fourth and see that it learns who the primaries are
    # from the other nodes
    create(names[3])
    checkProtocolInstanceSetup(looper, nodes, customTimeout=10)
    stopNodes(nodes, looper)


def testNodesConnectWhenTheyAllStartAtOnce(allPluginsPath, tdirAndLooper,
                                           nodeReg, conf):
    tdir, looper = tdirAndLooper
    nodes = []
    if conf.UseZStack:
        names = list(nodeReg.keys())
        genKeys(tdir, names + [_+CLIENT_STACK_SUFFIX for _ in names])

    for name in nodeReg:
        node = TestNode(name, nodeReg, basedirpath=tdir,
                        pluginPaths=allPluginsPath)
        looper.add(node)
        nodes.append(node)
    looper.run(checkNodesConnected(nodes))
    stopNodes(nodes, looper)


# @pytest.mark.parametrize("x10", range(1, 11))
# def testNodesComingUpAtDifferentTimes(x10):
def testNodesComingUpAtDifferentTimes(allPluginsPath, tdirAndLooper,
                                      nodeReg, conf):
    console = getConsole()
    console.reinit(flushy=True, verbosity=console.Wordage.verbose)
    tdir, looper = tdirAndLooper

    nodes = []

    names = list(nodeReg.keys())
    if conf.UseZStack:
        genKeys(tdir, names + [_+CLIENT_STACK_SUFFIX for _ in names])

    shuffle(names)
    waits = [randint(1, 10) for _ in names]
    rwaits = [randint(1, 10) for _ in names]

    for i, name in enumerate(names):
        node = TestNode(name, nodeReg, basedirpath=tdir,
                        pluginPaths=allPluginsPath)
        looper.add(node)
        nodes.append(node)
        looper.runFor(waits[i])
    looper.run(checkNodesConnected(nodes,
                                   customTimeout=10))
    logger.debug("connects")
    logger.debug("node order: {}".format(names))
    logger.debug("waits: {}".format(waits))

    stopNodes(nodes, looper)

    # # Giving some time for sockets to close, use eventually
    # time.sleep(1)

    for i, n in enumerate(nodes):
        n.start(looper.loop)
        looper.runFor(rwaits[i])
    looper.runFor(3)
    looper.run(checkNodesConnected(nodes,
                                   customTimeout=10))
    stopNodes(nodes, looper)
    logger.debug("reconnects")
    logger.debug("node order: {}".format(names))
    logger.debug("rwaits: {}".format(rwaits))


def testNodeConnection(allPluginsPath, tdirAndLooper, nodeReg, conf):
    console = getConsole()
    console.reinit(flushy=True, verbosity=console.Wordage.verbose)
    tdir, looper = tdirAndLooper
    names = ["Alpha", "Beta"]
    if conf.UseZStack:
        genKeys(tdir, names + [_+CLIENT_STACK_SUFFIX for _ in names])

    logger.debug(names)
    nrg = {n: nodeReg[n] for n in names}
    A, B = [TestNode(name, nrg, basedirpath=tdir,
                     pluginPaths=allPluginsPath)
            for name in names]
    looper.add(A)
    looper.runFor(4)
    logger.debug("wait done")
    looper.add(B)
    looper.runFor(4)
    looper.run(checkNodesConnected([A, B]))
    looper.stopall()
    A.start(looper.loop)
    looper.runFor(4)
    B.start(looper.loop)
    looper.run(checkNodesConnected([A, B]))
    stopNodes([A, B], looper)


def testNodeRemoveUnknownRemote(allPluginsPath, tdirAndLooper, nodeReg, conf):
    """
    The nodes Alpha and Beta know about each other so they should connect but
    they should remove remote for C when it tries to connect to them
    """

    tdir, looper = tdirAndLooper
    names = ["Alpha", "Beta"]
    logger.debug(names)

    if conf.UseZStack:
        _names = names + ['Gamma']
        genKeys(tdir, _names + [_+CLIENT_STACK_SUFFIX for _ in _names])

    nrg = {n: nodeReg[n] for n in names}
    A, B = [TestNode(name, nrg, basedirpath=tdir,
                     pluginPaths=allPluginsPath)
            for name in names]
    for node in (A, B):
        looper.add(node)
    looper.run(checkNodesConnected([A, B]))

    C = TestNode("Gamma", {**nrg, **{"Gamma": nodeReg["Gamma"]}},
                 basedirpath=tdir, pluginPaths=allPluginsPath)
    looper.add(C)

    def chk():
        assert not C.nodestack.isKeySharing
    timeout = waits.expectedPoolGetReadyTimeout(len(nodeReg))
    looper.run(eventually(chk, retryWait=2, timeout=timeout))
    stopNodes([C, ], looper)

    def chk():
        assert C.name not in B.nodestack.nameRemotes
        assert C.name not in A.nodestack.nameRemotes

    timeout = waits.expectedNodeInterconnectionTime(len(nodeReg))
    looper.run(eventually(chk, retryWait=2, timeout=timeout))
    stopNodes([A, B], looper)
