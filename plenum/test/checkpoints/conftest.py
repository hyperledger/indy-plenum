import pytest

from plenum.test.conftest import getValueFromModule
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected
from plenum.test.batching_3pc.conftest import tconf


@pytest.fixture(scope="module")
def chkFreqPatched(tconf, request):
    oldChkFreq = tconf.CHK_FREQ
    oldLogSize = tconf.LOG_SIZE

    tconf.CHK_FREQ = getValueFromModule(request, "CHK_FREQ", 2)
    tconf.LOG_SIZE = 2 * tconf.CHK_FREQ

    def reset():
        tconf.CHK_FREQ = oldChkFreq
        tconf.LOG_SIZE = oldLogSize

    request.addfinalizer(reset)

    return tconf


@pytest.fixture(scope="module")
def reqs_for_checkpoint(chkFreqPatched):
    return chkFreqPatched.CHK_FREQ * chkFreqPatched.Max3PCBatchSize


@pytest.fixture(scope="module")
def reqs_for_logsize(chkFreqPatched):
    return chkFreqPatched.LOG_SIZE * chkFreqPatched.Max3PCBatchSize
