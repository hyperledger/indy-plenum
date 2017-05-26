from itertools import product

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

    """
    A and D will see Nominations from B and C 3 seconds late
    B and C will see Nominations from A and D 5 seconds late
    """

    def delay(msg_type, frm, to, by):
        for f, t in product(frm, to):
            t.nodeIbStasher.delay(delayerMsgTuple(by, msg_type, f.name))

    delay(Nomination, frm=[B, C], to=[A, D], by=3)
    delay(Nomination, frm=[A, D], to=[B, C], by=5)


def test_reelection3(setup, looper, keySharedNodes):
    """
    A delays self nomination, gets Nom from D.

    A Reelection message received by a node that has already selected a primary
    should have the recipient send back who it picked as primary. The node
    proposing reelection when it sees f+1 consistent PRIMARY msgs from other
    nodes should accept that node as PRIMARY.

    """
    looper.run(checkNodesConnected(keySharedNodes))
    ensureElectionsDone(looper, keySharedNodes)
