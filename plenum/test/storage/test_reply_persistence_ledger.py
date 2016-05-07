import os

import pytest
from ledger.ledger import Ledger

from ledger.compact_merkle_tree import CompactMerkleTree
from plenum.test.storage.helper import checkReplyIsPersisted


@pytest.yield_fixture(scope="module")
def nodeSetLedger(nodeSet, tdir):
    """
    Overrides the fixture from conftest.py
    """
    for n in nodeSet:
        dirPath = os.path.join(tdir,n.name,"temp")
        if not os.path.exists(dirPath):
            os.makedirs(dirPath)
        n.txnStore = Ledger(CompactMerkleTree(), dirPath)
    yield nodeSet


@pytest.mark.skipif(True, reason="implementation of ledger changed")
def testReplyPersistedInLedger(nodeSetLedger, looper, replied1):
    checkReplyIsPersisted(nodeSetLedger, looper, replied1)
