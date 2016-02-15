from collections import OrderedDict
from zeno.test.helper import ensureElectionsDone
from zeno.test.testing_utils import setupTestLogging

setupTestLogging()
from zeno.cli.cli import Cli

nodeReg = OrderedDict([
    ('Alpha', ('127.0.0.1', 8001)),
    ('Beta', ('127.0.0.1', 8003)),
    ('Gamma', ('127.0.0.1', 8005)),
    ('Delta', ('127.0.0.1', 8007))
])

cliNodeReg = OrderedDict([
    ('AlphaC', ('127.0.0.1', 8002)),
    ('BetaC', ('127.0.0.1', 8004)),
    ('GammaC', ('127.0.0.1', 8006)),
    ('DeltaC', ('127.0.0.1', 8008))
])


def testAllNodesStartAtOnce(looper, tdir_for_func):
    # Add Alpha
    cli = Cli(looper, tdir_for_func, nodeReg, cliNodeReg)
    # Add Alpha
    cli.newNode('Alpha')
    na = cli.nodes['Alpha']  # type: Node
    # Add Beta
    cli.newNode('Beta')
    nb = cli.nodes['Beta']  # type: Node
    # Add Gamma
    cli.newNode('Gamma')
    ng = cli.nodes['Gamma']  # type: Node
    # Add Delta
    cli.newNode('Delta')
    nd = cli.nodes['Delta']  # type: Node

    ensureElectionsDone(looper, [na, nb, ng, nd], retryWait=10, timeout=60)
    # Add Client
    cli.newClient('myclient')
    # Send message to nodes
    cli.sendMsg('myclient', '{"Me": "So cool"}')
    client = cli.clients['myclient']  # type: Client
    # Get all replies for node, corresponding
    replies = [rep for rep in client.getRepliesFromAllNodes(1) if rep.reqId == 1]
    assert len(replies) > 0


def assertTrue(ev, av):
    assert ev == av