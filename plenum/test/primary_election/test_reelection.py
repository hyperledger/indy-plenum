from itertools import product

import pytest

from plenum.common.types import Nomination
from plenum.test.delayers import delayerMsgTuple
# from plenum.test.pool_transactions.conftest import clientAndWallet1, \
#     client1, wallet1, client1Connected, looper
from plenum.test.test_node import ensureElectionsDone, checkNodesConnected


def fill_counters(nodes):
    resend_call_counts = 0
    reelec_call_counts = 0
    for node in nodes:
        resend_call_counts += node.elector.spylog.count(
            node.elector.resend_primary.__name__)
        reelec_call_counts += node.elector.spylog.count(
            node.elector.sendReelection.__name__)
    return resend_call_counts, reelec_call_counts


@pytest.fixture(scope="module")
def setup(startedNodes):
    A, B, C, D = startedNodes.nodes.values()
    # A.delaySelfNomination(5)
    # A.nodeIbStasher.delay(delayerMsgTuple(3, Nomination, B.name))
    # A.delaySelfNomination(2)

    """
    A and D will see Nominations from B and C 3 seconds late
    B and C will see Nominations from A and D 5 seconds late
    """

    counters = fill_counters(startedNodes)

    def delay(msg_type, frm, to, by):
        for f, t in product(frm, to):
            t.nodeIbStasher.delay(delayerMsgTuple(by, msg_type, f.name, 0))

    delay(Nomination, frm=[A,], to=[B, C, D], by=2)
    delay(Nomination, frm=[B], to=[A, C, D], by=2)
    delay(Nomination, frm=[C], to=[A, B, D], by=2)
    delay(Nomination, frm=[D], to=[A, B, C], by=2)
    return counters


def test_reelection3(setup, looper, keySharedNodes):
    """
    Each node votes for itself, this should result in some node reaching
    re-election and some nodes sending a primary.

    A Reelection message received by a node that has already selected a primary
    should have the recipient send back who it picked as primary. The node
    proposing reelection when it sees f+1 consistent PRIMARY msgs from other
    nodes should accept that node as PRIMARY.

    """
    old_counter_resend, old_counter_relec = setup
    looper.run(checkNodesConnected(keySharedNodes))
    ensureElectionsDone(looper, keySharedNodes)
    # Check that both the number of call to `sendReelection` and
    # `sendReelection` have increased
    new_counter_resend, new_counter_relec = fill_counters(keySharedNodes)
    assert new_counter_resend > old_counter_resend
    assert new_counter_relec > old_counter_relec
