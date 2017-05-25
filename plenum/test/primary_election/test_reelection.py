import pytest

from plenum.common.types import Nomination
from plenum.test.delayers import delayerMsgTuple
# from plenum.test.pool_transactions.conftest import clientAndWallet1, \
#     client1, wallet1, client1Connected, looper
from plenum.test.test_node import ensureElectionsDone, checkNodesConnected



@pytest.fixture()
def setup(startedNodes):
    A, B, C, D = startedNodes.nodes.values()
    # A.delaySelfNomination(5)
    # A.nodeIbStasher.delay(delayerMsgTuple(3, Nomination, B.name))
    A.delaySelfNomination(2)

    for n in [C, B]:
        A.nodeIbStasher.delay(delayerMsgTuple(3, Nomination, n.name))
        D.nodeIbStasher.delay(delayerMsgTuple(3, Nomination, n.name))
    for n in [C, B]:
        n.nodeIbStasher.delay(delayerMsgTuple(5, Nomination, D.name))
        n.nodeIbStasher.delay(delayerMsgTuple(5, Nomination, A.name))


def test_reelection3(setup, looper, keySharedNodes):
    """
    A delays self nomination, gets Nom from D.
    """
    looper.run(checkNodesConnected(keySharedNodes))
    ensureElectionsDone(looper, keySharedNodes)
