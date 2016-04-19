import asyncio
import os

from plenum.common.types import Reply
from plenum.common.util import checkPortAvailable, getlogger
from plenum.persistence.mongodb_server import MongoDBServer
from plenum.persistence.rethinkdb_server import RethinkDB
from plenum.test.helper import genHa
from plenum.test.storage.helper import getDBPathForMongo, TestRethinkDB

loop = asyncio.get_event_loop()
logger = getlogger()


def testRethinkDB(tdir):
    """
    This test requires 1 Rethink DB instances running at port: 28015
    """

    def f(port, tdir):
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

    async def go():
        port = genHa(count=1)[1]
        rdb = f(port, tdir)
        rdb.start(loop)
        identifier = "testClientId"
        txnId = "txnId"
        reply = Reply(1, 1, "theresult")
        # rdb.reset()
        # rdb._bootstrapDB()
        # insert an entry
        await rdb.append(identifier, reply, txnId)
        txn_in_db = await rdb.get(identifier, reply.reqId)
        assert txn_in_db == reply
        assert await rdb.size() == 1
        rdb.stop()

    loop.run_until_complete(go())


def testMongoDB():
    port = genHa(count=1)[1]

    async def go():
        mdb = MongoDBServer(host="127.0.0.1",
                            port=port,
                            dirpath=getDBPathForMongo(port))
        mdb.start(loop)
        identifier = "testClientId"
        txnId = "txnId"
        reply = Reply(1, 1, "theresult")
        await mdb.reset()
        # insert an entry
        await mdb.append(identifier, reply, txnId)
        txn_in_db = await mdb.get(identifier, reply.reqId)
        assert txn_in_db == reply
        assert await mdb.size() == 1
        mdb.stop()

    loop.run_until_complete(go())
    loop.close()
