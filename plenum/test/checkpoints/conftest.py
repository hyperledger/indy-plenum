import pytest

from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected

CHK_FREQ = 3


@pytest.fixture(scope="module")
def chkFreqPatched(tconf, request):
    oldChkFreq = tconf.CHK_FREQ
    tconf.CHK_FREQ = CHK_FREQ
    tconf.LOG_SIZE = 3*tconf.CHK_FREQ

    def reset():
        tconf.CHK_FREQ = oldChkFreq
        tconf.LOG_SIZE = 3*tconf.CHK_FREQ

    request.addfinalizer(reset)

    return tconf
