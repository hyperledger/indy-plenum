from random import shuffle, randint
from tempfile import TemporaryDirectory

from ioflo.aid import getConsole

from zeno.common.looper import Looper
from zeno.common.util import getlogger
from zeno.server.node import Node
from zeno.test.helper import checkNodesConnected, checkProtocolInstanceSetup

logger = getlogger()

whitelist = ['discarding message']

nodeReg = {
    'Alpha': ('127.0.0.1', 7560),
    'Beta': ('127.0.0.1', 7562),
    'Gamma': ('127.0.0.1', 7564),
    'Delta': ('127.0.0.1', 7566)}


def testNodesConnectsWhenOneNodeIsLate():
    with TemporaryDirectory() as td:
        with Looper() as looper:
            nodes = []
            names = list(nodeReg.keys())
            logger.debug("Node names: {}".format(names))

            def create(name):
                node = Node(name, nodeReg, basedirpath=td)
                looper.add(node)
                node.startKeySharing()
                nodes.append(node)

            for name in names[:3]:
                create(name)

            looper.run(checkNodesConnected(nodes))

            # wait for the election to complete with the first three nodes
            looper.runFor(10)

            # create the fourth and see that it learns who the primaries are
            # from the other nodes
            create(names[3])
            checkProtocolInstanceSetup(looper, nodes, timeout=10)


def testNodesConnectWhenTheyAllStartAtOnce():
    with TemporaryDirectory() as td:
        with Looper() as looper:
            nodes = []
            for name in nodeReg:
                node = Node(name, nodeReg, basedirpath=td)
                looper.add(node)
                node.startKeySharing()
                nodes.append(node)
            looper.run(checkNodesConnected(nodes))


# @pytest.mark.parametrize("x10", range(1, 11))
# def testNodesComingUpAtDifferentTimes(x10):
def testNodesComingUpAtDifferentTimes():
    console = getConsole()
    console.reinit(flushy=True, verbosity=console.Wordage.verbose)
    with TemporaryDirectory() as td:
        print("temporary directory: {}".format(td))
        with Looper() as looper:
            nodes = []

            names = list(nodeReg.keys())
            shuffle(names)
            waits = [randint(1, 10) for _ in names]
            rwaits = [randint(1, 10) for _ in names]

            for i, name in enumerate(names):
                node = Node(name, nodeReg, basedirpath=td)
                looper.add(node)
                node.startKeySharing()
                nodes.append(node)
                looper.runFor(waits[i])
            looper.run(checkNodesConnected(nodes,
                                           overrideTimeout=10))
            print("connects")
            print("node order: {}".format(names))
            print("waits: {}".format(waits))

            for n in nodes:
                n.stop()
            for i, n in enumerate(nodes):
                n.start()
                looper.runFor(rwaits[i])
            looper.runFor(3)
            looper.run(checkNodesConnected(nodes,
                                           overrideTimeout=10))
            print("reconnects")
            print("node order: {}".format(names))
            print("rwaits: {}".format(rwaits))


def testNodeConnection():
    console = getConsole()
    console.reinit(flushy=True, verbosity=console.Wordage.verbose)
    with TemporaryDirectory() as td:
        print("temporary directory: {}".format(td))
        with Looper() as looper:
            names = ["Alpha", "Beta"]
            print(names)
            nrg = {n: nodeReg[n] for n in names}
            A, B = [Node(name, nrg, basedirpath=td)
                    for name in names]
            looper.add(A)
            A.startKeySharing()
            looper.runFor(4)
            print("wait done")
            looper.add(B)
            B.startKeySharing()
            looper.runFor(4)
            looper.run(checkNodesConnected([A, B]))
            looper.stopall()
            A.start()
            looper.runFor(4)
            B.start()
            looper.run(checkNodesConnected([A, B]))

            # TODO need to vary the times between nodes coming up in order to
            # TODO     see where issues are
