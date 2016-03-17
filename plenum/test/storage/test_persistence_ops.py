import asyncio

from plenum.common.request_types import Reply
from plenum.storage.mongodb_server import MongoDBServer
from plenum.storage.rethinkdb_server import RethinkDB
from plenum.test.storage.helper import getDBPathForMongo

loop = asyncio.get_event_loop()


def testRethinkDB(tdir):
    """
    This test requires 1 Rethink DB instances running at port: 28015
    """

    async def go():
        rdb = RethinkDB(host="127.0.0.1", port=28015, dirpath=tdir)
        rdb.start(loop)
        identifier = "testClientId"
        txnId = "txnId"
        reply = Reply(1, 1, "theresult")
        # rdb.reset()
        # rdb._bootstrapDB()
        # insert an entry
        await rdb.insertTxn(identifier, reply, txnId)
        txn_in_db = await rdb.getTxn(identifier, reply.reqId)
        assert txn_in_db == reply
        rdb.stop()

    loop.run_until_complete(go())


def testMongoDB():
    port = 27017

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
        await mdb.insertTxn(identifier, reply, txnId)
        txn_in_db = await mdb.getTxn(identifier, reply.reqId)
        assert txn_in_db == reply
        mdb.stop()

    loop.run_until_complete(go())
    loop.close()
