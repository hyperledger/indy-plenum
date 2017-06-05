import pytest

from plenum.common.util import adict
from plenum.test.delayers import delayNonPrimaries
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, \
    waitForViewChange
from plenum.test.test_node import ensureElectionsDone, get_master_primary_node


@pytest.fixture()
def viewNo(nodeSet):
    viewNos = set()
    for n in nodeSet:
        viewNos.add(n.viewNo)
    assert len(viewNos) == 1
    return viewNos.pop()


@pytest.fixture()
def simulate_slow_master(nodeSet, looper, up, wallet1, client1, viewNo):
    def _():
        m_primary_node = get_master_primary_node(list(nodeSet.nodes.values()))
        # Delay processing of PRE-PREPARE from all non primary replicas of master
        # so master's performance falls and view changes
        delayNonPrimaries(nodeSet, 0, 10)

        sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 4)

        try:
            waitForViewChange(looper, nodeSet, expectedViewNo=viewNo+1)
        except AssertionError as e:
            raise RuntimeError('view did not change') from e
        ensureElectionsDone(looper=looper, nodes=nodeSet)
        new_m_primary_node = get_master_primary_node(list(nodeSet.nodes.values()))
        return adict(old=m_primary_node, new=new_m_primary_node)
    return _
