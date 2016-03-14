import os

import pytest
from ledger.immutable_store.ledger import Ledger
from ledger.immutable_store.leveldb_ledger import LevelDBLedger
from ledger.immutable_store.merkle import CompactMerkleTree

from plenum.test.storage.helper import checkReplyIsPersisted

@pytest.yield_fixture(scope="module")
def nodeSetMongo(nodeSet, tdir):
    """
    Overrides the fixture from conftest.py
    """
    for n in nodeSet:
        dir = os.path.join(tdir,n.name)
        if not os.path.exists(dir):
            os.makedirs(dir)
        ledger_db = LevelDBLedger(dir)
        # ledger_db._db.close()
        n.txnStore = Ledger(CompactMerkleTree(), ledger_db)
    yield nodeSet


def testReplyPersistedInLevelDb(nodeSetMongo, looper, replied1):
    checkReplyIsPersisted(nodeSetMongo, looper, replied1)