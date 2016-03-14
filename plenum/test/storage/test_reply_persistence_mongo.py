import pytest

from plenum.storage.mongodb_server import MongoDBServer
from plenum.test.storage.helper import checkReplyIsPersisted, getDBPathForMongo


@pytest.yield_fixture(scope="module")
def nodeSetMongo(nodeSet):
    port = 27017
    """
    Overrides the fixture from conftest.py
    """
    for n in nodeSet:
        n.txnStore = MongoDBServer(host='127.0.0.1',
                                   port=port,
                                   dirpath=getDBPathForMongo(port))
        port += 1
    yield nodeSet


def testReplyPersistedInMongo(nodeSetMongo, looper, replied1):
    checkReplyIsPersisted(nodeSetMongo, looper, replied1)