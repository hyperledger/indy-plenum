import pytest

from plenum.common.util import get_utc_epoch
from plenum.server.quorums import Quorums
from plenum.server.view_change.view_changer import ViewChanger
from plenum.test.conftest import getValueFromModule
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
    node_stack = FakeSomething(
        name="fake stack",
        connecteds={"Alpha", "Beta", "Gamma", "Delta"}
    )
    monitor = FakeSomething(
        isMasterDegraded=lambda: False,
    )
    node = FakeSomething(
        name="SomeNode",
        viewNo=request.param,
        quorums=Quorums(getValueFromModule(request, 'nodeCount', default=4)),
        nodestack=node_stack,
        utc_epoch=lambda *args: get_utc_epoch(),
        config=tconf,
        monitor=monitor,
        discard=lambda a, b, c: print(b),
    )
    view_changer = ViewChanger(node)
    return view_changer
