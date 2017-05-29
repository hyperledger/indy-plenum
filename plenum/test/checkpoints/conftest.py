import pytest

from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected
from plenum.test.batching_3pc.conftest import tconf

CHK_FREQ_FACTOR = 3


@pytest.fixture(scope="module")
def chkFreqPatched(tconf, request):
    oldChkFreq = tconf.CHK_FREQ
    oldLogSize = tconf.LOG_SIZE

    tconf.CHK_FREQ = CHK_FREQ_FACTOR*tconf.Max3PCBatchSize
    tconf.LOG_SIZE = 3*tconf.CHK_FREQ

    def reset():
        tconf.CHK_FREQ = oldChkFreq
        tconf.LOG_SIZE = oldLogSize

    request.addfinalizer(reset)

    return tconf
