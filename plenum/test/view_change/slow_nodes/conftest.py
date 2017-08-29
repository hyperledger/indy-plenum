import pytest


@pytest.fixture(scope="module")
def client(looper, txnPoolNodeSet, client1, client1Connected):
    return client1Connected
