import pytest


@pytest.fixture(scope="module")
def tconf(tconf):
    old = tconf.CatchupTransactionsTimeout

    tconf.CatchupTransactionsTimeout = 2
    yield tconf

    tconf.CatchupTransactionsTimeout = old
