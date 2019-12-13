import functools
from collections import deque

import pytest

from plenum.common.event_bus import InternalBus
from plenum.common.timer import QueueTimer
from plenum.common.util import get_utc_epoch
from plenum.server.node import Node
from plenum.server.quorums import Quorums
from plenum.server.view_change.node_view_changer import create_view_changer
from plenum.test.conftest import getValueFromModule
from plenum.test.primary_selection.test_view_changer_primary_selection import FakeNode
from plenum.test.test_node import getRequiredInstances
from plenum.test.testing_utils import FakeSomething


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
def fake_view_changer(request, tconf):
    node_count = 4
    node_stack = FakeSomething(
        name="fake stack",
        connecteds={"Alpha", "Beta", "Gamma", "Delta"},
        conns={"Alpha", "Beta", "Gamma", "Delta"}
    )
    monitor = FakeSomething(
        isMasterDegraded=lambda: False,
        areBackupsDegraded=lambda: [],
        prettymetrics=''
    )
    node = FakeSomething(
        name="SomeNode",
        timer=QueueTimer(),
        viewNo=request.param,
        quorums=Quorums(getValueFromModule(request, 'nodeCount', default=node_count)),
        nodestack=node_stack,
        utc_epoch=lambda *args: get_utc_epoch(),
        config=tconf,
        monitor=monitor,
        discard=lambda a, b, c, d: print(b),
        primaries_disconnection_times=[None] * getRequiredInstances(node_count),
        master_primary_name='Alpha',
        master_replica=FakeSomething(instId=0,
                                     viewNo=request.param,
                                     _consensus_data=FakeSomething(view_no=request.param,
                                                                   waiting_for_new_view=False)),
        nodeStatusDB=None
    )
    view_changer = create_view_changer(node)
    # TODO: This is a hack for tests compatibility, do something better
    view_changer.node = node
    return view_changer


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
