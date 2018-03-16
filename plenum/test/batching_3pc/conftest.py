import pytest

from plenum.test.conftest import getValueFromModule


@pytest.fixture(scope="module")
def tconf(tconf, request):
    oldSize = tconf.Max3PCBatchSize
    tconf.Max3PCBatchSize = getValueFromModule(request, "Max3PCBatchSize", 10)

    def reset():
        tconf.Max3PCBatchSize = oldSize

    request.addfinalizer(reset)
    return tconf
