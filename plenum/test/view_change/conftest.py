import functools

import pytest

from plenum.server.node import Node
from plenum.test.conftest import getValueFromModule
from plenum.test.primary_selection.test_view_changer_primary_selection import FakeNode


@pytest.fixture()
def viewNo(txnPoolNodeSet):
    viewNos = set()
    for n in txnPoolNodeSet:
        viewNos.add(n.viewNo)
    assert len(viewNos) == 1
    return viewNos.pop()


@pytest.fixture(scope="module")
def perf_chk_patched(tconf, request):
    old_val = tconf.PerfCheckFreq
    tconf.PerfCheckFreq = getValueFromModule(request, "PerfCheckFreq", 20)

    def reset():
        tconf.PerfCheckFreq = old_val

    request.addfinalizer(reset)
    return tconf


@pytest.fixture(scope='function', params=[0, 10])
def fake_node(tdir, tconf, request):
    node = FakeNode(tdir, config=tconf)
    node.msgHasAcceptableViewNo = Node.msgHasAcceptableViewNo
    node._is_initial_view_change_now = functools.partial(Node._is_initial_view_change_now, node)
    node.msgsForFutureViews = {}
    node.set_view_for_replicas = lambda a: None
    node.master_replica._consensus_data.view_no = request.param
    node.last_completed_view_no = request.param
    return node
