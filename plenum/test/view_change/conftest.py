import pytest

from plenum.test.conftest import getValueFromModule


@pytest.fixture()
def viewNo(nodeSet):
    viewNos = set()
    for n in nodeSet:
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
