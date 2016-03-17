import os

import pytest

from plenum.common.util import getlogger, checkPortAvailable
from plenum.storage.rethinkdb_server import RethinkDB
from plenum.test.helper import genHa
from plenum.test.storage.helper import checkReplyIsPersisted, TestRethinkDB

logger = getlogger()

host = "127.0.0.1"
nodeCount = 4


@pytest.yield_fixture(scope="module")
def nodeSetRethink(tdir, nodeSet):
    port = 28015
    """
    Overrides the fixture from conftest.py
    """

    def go(port, tdir):
        rdir = os.path.join(tdir, str(port))
        if not os.path.exists(rdir):
            os.makedirs(rdir)
        try:
            return TestRethinkDB(host='127.0.0.1',
                             port=port,
                             dirpath=rdir)
        except Exception as ex:
            logger.debug("Rethinkdb encountered exception {} on port".format(ex, port))
            raise ex
        # if checkPortAvailable(("127.0.0.1", port)):
        #     rdir = os.path.join(tdir, str(port))
        #     if not os.path.exists(rdir):
        #         os.makedirs(rdir)
        #     try:
        #         return RethinkDB(host='127.0.0.1',
        #                          port=port,
        #                          dirpath=rdir)
        #     except Exception:
        #         port += 10
        #         go(port, tdir)
        # else:
        #     port += 10
        #     go(port, tdir)
    for n in nodeSet:
        port = genHa(count=1)[1]
        n.txnStore = go(port, tdir)
        # port += 1
    yield nodeSet


def testReplyPersistedInRethink(nodeSetRethink, looper, replied1):
    checkReplyIsPersisted(nodeSetRethink, looper, replied1)
