import pytest
from ct.crypto.merkle import CompactMerkleTree

from plenum.test.storage.helper import checkReplyIsPersisted

@pytest.yield_fixture(scope="module")
def nodeSetMongo(nodeSet):
    port = 27017
    """
    Overrides the fixture from conftest.py
    """
    for n in nodeSet:
        ledger_db = LevelDBLedger("/tmp/testLedger")
        n.txnStore = Ledger(CompactMerkleTree(), ledger_db)
        port += 1
    yield nodeSet


def testReplyPersistedInMongo(nodeSetMongo, looper, replied1):
    checkReplyIsPersisted(nodeSetMongo, looper, replied1)