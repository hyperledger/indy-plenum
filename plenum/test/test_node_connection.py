from random import shuffle, randint

import pytest
from ioflo.aid import getConsole

from plenum.common.keygen_utils import initNodeKeysForBothStacks, tellKeysToOthers
from plenum.common.util import randomString
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.common.types import NodeDetail
from plenum.test import waits
from plenum.test.helper import stopNodes
from plenum.test.test_node import TestNode, checkNodesConnected, \
    ensureElectionsDone
from stp_core.network.port_dispenser import genHa
from plenum.common.config_helper import PNodeConfigHelper

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


def initLocalKeys(tdir_for_func, tconf_for_func, nodeReg):
    for nName in nodeReg.keys():
        sigseed = randomString(32).encode()
        config_helper = PNodeConfigHelper(nName, tconf_for_func, chroot=tdir_for_func)
        initNodeKeysForBothStacks(nName, config_helper.keys_dir, sigseed, override=True)
        logger.debug('Created keys for {}'.format(nName))


@pytest.mark.skip(reason='INDY-109. Intermittent failures')
def testNodesConnectsWhenOneNodeIsLate(allPluginsPath, tdir_for_func, tconf_for_func,
                                       looper_without_nodeset_for_func,
                                       nodeReg):
    looper = looper_without_nodeset_for_func
    initLocalKeys(tdir_for_func, tconf_for_func, nodeReg)

    nodes = []
    names = list(nodeReg.keys())
    logger.debug("Node names: {}".format(names))

    def create(name):
        config_helper = PNodeConfigHelper(name, tconf_for_func, chroot=tdir_for_func)
        node = TestNode(name, nodeReg,
                        config_helper=config_helper,
                        config=tconf_for_func,
                        pluginPaths=allPluginsPath)
        nodes.append(node)
        return node

    for name in names:
        create(name)

    logger.debug("Creating keys")

    for node in nodes:
        tellKeysToOthers(node, nodes)

    for node in nodes[:3]:
        looper.add(node)

    looper.run(checkNodesConnected(nodes[:3]))

    # wait for the election to complete with the first three nodes
    ensureElectionsDone(looper, nodes[:3], numInstances=2)

    # start the fourth and see that it learns who the primaries are
    # from the other nodes
    looper.add(nodes[3])

    # ensure election is done for updated pool
    ensureElectionsDone(looper, nodes)
    stopNodes(nodes, looper)


def testNodesConnectWhenTheyAllStartAtOnce(allPluginsPath, tdir_for_func, tconf_for_func,
                                           looper_without_nodeset_for_func,
                                           nodeReg):
    looper = looper_without_nodeset_for_func
    nodes = []

    initLocalKeys(tdir_for_func, tconf_for_func, nodeReg)

    for name in nodeReg:
        config_helper = PNodeConfigHelper(name, tconf_for_func, chroot=tdir_for_func)
        node = TestNode(name, nodeReg,
                        config_helper=config_helper,
                        config=tconf_for_func,
                        pluginPaths=allPluginsPath)
        nodes.append(node)

    for node in nodes:
        tellKeysToOthers(node, nodes)

    for node in nodes:
        looper.add(node)

    looper.run(checkNodesConnected(nodes))
    stopNodes(nodes, looper)


# @pytest.mark.parametrize("x10", range(1, 11))
# def testNodesComingUpAtDifferentTimes(x10):
def testNodesComingUpAtDifferentTimes(allPluginsPath, tdir_for_func, tconf_for_func,
                                      looper_without_nodeset_for_func,
                                      nodeReg):
    console = getConsole()
    console.reinit(flushy=True, verbosity=console.Wordage.verbose)
    looper = looper_without_nodeset_for_func

    initLocalKeys(tdir_for_func, tconf_for_func, nodeReg)

    nodes = []

    names = list(nodeReg.keys())

    shuffle(names)
    waits = [randint(1, 10) for _ in names]
    rwaits = [randint(1, 10) for _ in names]

    for name in names:
        config_helper = PNodeConfigHelper(name, tconf_for_func, chroot=tdir_for_func)
        node = TestNode(name, nodeReg,
                        config_helper=config_helper,
                        config=tconf_for_func,
                        pluginPaths=allPluginsPath)
        nodes.append(node)

    for node in nodes:
        tellKeysToOthers(node, nodes)

    for i, node in enumerate(nodes):
        looper.add(node)
        looper.runFor(waits[i])
    looper.run(checkNodesConnected(nodes))
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
    looper.run(checkNodesConnected(nodes))
    stopNodes(nodes, looper)
    logger.debug("reconnects")
    logger.debug("node order: {}".format(names))
    logger.debug("rwaits: {}".format(rwaits))


def testNodeConnection(allPluginsPath, tdir_for_func, tconf_for_func,
                       looper_without_nodeset_for_func,
                       nodeReg):
    console = getConsole()
    console.reinit(flushy=True, verbosity=console.Wordage.verbose)
    looper = looper_without_nodeset_for_func
    names = ["Alpha", "Beta"]
    nrg = {n: nodeReg[n] for n in names}
    initLocalKeys(tdir_for_func, tconf_for_func, nrg)

    logger.debug(names)
    nodes = []
    for name in names:
        config_helper = PNodeConfigHelper(name, tconf_for_func, chroot=tdir_for_func)
        node = TestNode(name, nrg,
                        config_helper=config_helper,
                        config=tconf_for_func,
                        pluginPaths=allPluginsPath)
        nodes.append(node)

    for node in nodes:
        tellKeysToOthers(node, nodes)

    A, B = nodes
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


def testNodeRemoveUnknownRemote(allPluginsPath, tdir_for_func, tconf_for_func,
                                looper_without_nodeset_for_func,
                                nodeReg):
    """
    The nodes Alpha and Beta know about each other so they should connect but
    they should remove remote for C when it tries to connect to them
    """

    looper = looper_without_nodeset_for_func
    names = ["Alpha", "Beta"]
    nrg = {n: nodeReg[n] for n in names}
    initLocalKeys(tdir_for_func, tconf_for_func, nrg)
    logger.debug(names)

    nodes = []
    for name in names:
        config_helper = PNodeConfigHelper(name, tconf_for_func, chroot=tdir_for_func)
        node = TestNode(name, nrg,
                        config_helper=config_helper,
                        config=tconf_for_func,
                        pluginPaths=allPluginsPath)
        nodes.append(node)

    for node in nodes:
        tellKeysToOthers(node, nodes)

    A, B = nodes
    for node in nodes:
        looper.add(node)
    looper.run(checkNodesConnected(nodes))

    name = "Gamma"
    initLocalKeys(tdir_for_func, tconf_for_func, {name: nodeReg[name]})
    config_helper = PNodeConfigHelper(name, tconf_for_func, chroot=tdir_for_func)
    C = TestNode(name, {**nrg, **{name: nodeReg[name]}},
                 config_helper=config_helper,
                 config=tconf_for_func,
                 pluginPaths=allPluginsPath)
    for node in nodes:
        tellKeysToOthers(node, [C, ])

    looper.add(C)
    looper.runFor(5)

    stopNodes([C, ], looper)

    timeout = waits.expectedPoolInterconnectionTime(len(nodeReg))
    stopNodes([A, B], looper)
