import pytest

from plenum.common.types import Primary, Nomination, Reelection
from plenum.test.delayers import delay
from plenum.test.test_node import checkNodesConnected, \
    checkProtocolInstanceSetup
from stp_core.loop.eventually import eventually


@pytest.fixture()
def case_6_setup(startedNodes):
    A, B, C, D = startedNodes.nodes.values()

    # A will get Nomination, Primary, Reelection from after elections get over
    for m in (Nomination, Primary, Reelection):
        delay(m, frm=B, to=A, howlong=120)

    # A will get Primary earlier than Nominates
    delay(Nomination, frm=(C, D), to=A, howlong=5)


# noinspection PyIncorrectDocstring
def test_primary_election_case6(case_6_setup, looper, keySharedNodes):
    """
    A is disconnected with B so A does not get any Nomination/Primary from
    B (simulated by a large delay). A gets Nominations delayed due to which is
    sends Primary only after it has received Primary from other 2 nodes.
    A should still be able to select a primary and the pool should function.
    """
    nodeSet = keySharedNodes
    A, B, C, D = nodeSet.nodes.values()
    looper.run(checkNodesConnected(nodeSet))

    inst_ids = (0, 1)

    def chk():
        # Check that each Primary is received by A before A has sent any Primary
        primary_recv_times = {
            i: [entry.starttime for entry in A.elector.spylog.getAll(
                A.elector.processPrimary) if entry.params['prim'].instId == i]
            for i in inst_ids
        }
        primary_send_times = {
            i: [entry.starttime for entry in A.elector.spylog.getAll(
                A.elector.sendPrimary) if entry.params['instId'] == 0]
            for i in inst_ids
        }

        for i in inst_ids:
            assert primary_send_times[i] > primary_recv_times[i]

    looper.run(eventually(chk, retryWait=1, timeout=10))
    checkProtocolInstanceSetup(looper=looper, nodes=nodeSet, retryWait=1)
