import pytest

from plenum.storage.mongodb_server import MongoDBServer
from plenum.test.helper import genHa
from plenum.test.storage.helper import checkReplyIsPersisted, getDBPathForMongo


@pytest.yield_fixture(scope="module")
def nodeSetMongo(nodeSet):
    """
    Overrides the fixture from conftest.py
    """
    for n in nodeSet:
        port = genHa(count=1)[1]
        n.txnStore = MongoDBServer(host='127.0.0.1',
                                   port=port,
                                   dirpath=getDBPathForMongo(port))
    yield nodeSet


def testReplyPersistedInMongo(nodeSetMongo, looper, replied1):
    checkReplyIsPersisted(nodeSetMongo, looper, replied1)
