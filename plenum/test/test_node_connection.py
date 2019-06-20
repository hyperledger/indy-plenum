from random import shuffle, randint

import pytest
from ioflo.aid import getConsole

from plenum.common.keygen_utils import initNodeKeysForBothStacks, tellKeysToOthers
from plenum.common.util import randomString
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.view_change.helper import start_stopped_node
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
    ensureElectionsDone(looper, nodes[:3], instances_list=range(2))

    # start the fourth and see that it learns who the primaries are
    # from the other nodes
    looper.add(nodes[3])

    # ensure election is done for updated pool
    ensureElectionsDone(looper, nodes)
    stopNodes(nodes, looper)
    for node in nodes:
        looper.removeProdable(node)


def testNodesConnectWhenTheyAllStartAtOnce(allPluginsPath, tdir_for_func, tconf_for_func,
                                           looper,
                                           txnPoolNodeSetNotStarted):
    nodes = txnPoolNodeSetNotStarted

    for node in nodes:
        tellKeysToOthers(node, nodes)

    for node in nodes:
        looper.add(node)

    looper.run(checkNodesConnected(nodes))
    stopNodes(nodes, looper)
    for node in nodes:
        looper.removeProdable(node)


# @pytest.mark.parametrize("x10", range(1, 11))
# def testNodesComingUpAtDifferentTimes(x10):
def testNodesComingUpAtDifferentTimes(allPluginsPath, tconf, tdir,
                                      tdir_for_func, tconf_for_func,
                                      looper, txnPoolNodeSetNotStarted):
    console = getConsole()
    console.reinit(flushy=True, verbosity=console.Wordage.verbose)

    nodes = txnPoolNodeSetNotStarted

    names = list(node.name for node in nodes)

    shuffle(names)
    waits = [randint(1, 10) for _ in names]
    rwaits = [randint(1, 10) for _ in names]

    for node in nodes:
        tellKeysToOthers(node, nodes)

    for i, node in enumerate(nodes):
        looper.add(node)
        looper.runFor(waits[i])
    looper.run(checkNodesConnected(nodes))
    logger.debug("connects")
    logger.debug("node order: {}".format(names))
    logger.debug("waits: {}".format(waits))

    current_node_set = set(nodes)
    for node in nodes:
        disconnect_node_and_ensure_disconnected(looper,
                                                current_node_set,
                                                node,
                                                timeout=len(nodes),
                                                stopNode=True)
        looper.removeProdable(node)
        current_node_set.remove(node)

    for i, node in enumerate(nodes):
        restarted_node = start_stopped_node(node, looper,
                                            tconf, tdir, allPluginsPath)
        current_node_set.add(restarted_node)
        looper.runFor(rwaits[i])

    looper.runFor(3)
    looper.run(checkNodesConnected(current_node_set))

    stopNodes(current_node_set, looper)
    logger.debug("reconnects")
    logger.debug("node order: {}".format(names))
    logger.debug("rwaits: {}".format(rwaits))
    for node in current_node_set:
        looper.removeProdable(node)


def testNodeConnection(allPluginsPath, tconf, tdir,
                       tdir_for_func, tconf_for_func,
                       looper, txnPoolNodeSetNotStarted):
    console = getConsole()
    console.reinit(flushy=True, verbosity=console.Wordage.verbose)

    nodes = txnPoolNodeSetNotStarted[:2]

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
    looper.removeProdable(A)
    looper.removeProdable(B)
    A = start_stopped_node(A, looper, tconf, tdir, allPluginsPath)
    looper.runFor(4)
    B = start_stopped_node(B, looper, tconf, tdir, allPluginsPath)
    looper.run(checkNodesConnected([A, B]))

    for node in txnPoolNodeSetNotStarted[2:]:
        looper.add(node)
    all_nodes = [A, B] + txnPoolNodeSetNotStarted[2:]
    looper.run(checkNodesConnected(all_nodes))
    stopNodes(all_nodes, looper)
    for node in all_nodes:
        looper.removeProdable(node)


def testNodeRemoveUnknownRemote(allPluginsPath, tdir_for_func, tconf_for_func,
                                looper,
                                txnPoolNodeSetNotStarted):
    """
    The nodes Alpha and Beta know about each other so they should connect but
    they should remove remote for C when it tries to connect to them
    """
    nodes = txnPoolNodeSetNotStarted[:2]

    for node in nodes:
        tellKeysToOthers(node, nodes)

    A, B = nodes
    for node in nodes:
        looper.add(node)
    looper.run(checkNodesConnected(nodes))

    C = txnPoolNodeSetNotStarted[2]
    for node in nodes:
        tellKeysToOthers(node, [C, ])

    looper.add(C)
    looper.runFor(5)

    stopNodes([C, ], looper)
    stopNodes([A, B], looper)
    for node in [A, B, C]:
        looper.removeProdable(node)
