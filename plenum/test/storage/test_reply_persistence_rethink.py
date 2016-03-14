import os

import pytest

from plenum.common.util import getlogger
from plenum.storage.rethinkdb_server import RethinkDB
from plenum.test.storage.helper import checkReplyIsPersisted

logger = getlogger()

host = "127.0.0.1"
nodeCount = 4


@pytest.yield_fixture(scope="module")
def nodeSetRethink(tdir, nodeSet):
    port = 28015
    """
    Overrides the fixture from conftest.py
    """
    for n in nodeSet:
        rdir = os.path.join(tdir, str(port))
        if not os.path.exists(rdir):
            os.makedirs(rdir)
        n.txnStore = RethinkDB(host='127.0.0.1',
                               port=port,
                               dirpath=rdir)
        port += 1
    yield nodeSet


def testReplyPersistedInRethink(nodeSetRethink, looper, replied1):
    checkReplyIsPersisted(nodeSetRethink, looper, replied1)

