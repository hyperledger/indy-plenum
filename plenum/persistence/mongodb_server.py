import os
from subprocess import call

from multiprocessing import Process

import time

import motor.motor_asyncio

from plenum.common.types import Reply, f
from plenum.common.util import getlogger, checkPortAvailable
from plenum.persistence.storage import Storage

logger = getlogger()


# Default port for mongodb is: 27017

class MongoDBServer(Storage):
    def __init__(self, host: str, port: int, dirpath: str):
        if not checkPortAvailable((host, port)):
            raise RuntimeError(
                "Address {}:{} already in use".format(host, port))
        self.host = host
        self.port = port
        self.dirpath = dirpath
        self.mongoClient = None
        self.dbName = "db{}".format(port)
        self.db = None
        self.processedReqTB = None
        self.txnTB = None

    def start(self, loop):
        nodeWaitTime = 2
        if not os.path.exists(self.dirpath):
            nodeWaitTime = 60
            os.makedirs(self.dirpath)

        DB_CMD = 'mongod'
        DB_ARGS = ('--dbpath', '{}'.format(self.dirpath),
                   '--port', '{}'.format(self.port))

        def startDB():
            try:
                call([DB_CMD, *DB_ARGS])
            except Exception as ex:
                raise RuntimeError("Could not call '{}'; is it installed?".
                                   format(DB_CMD)) from ex

        p = Process(target=startDB)
        p.start()
        # todo: Need a better way, this time.sleep() isn't good.
        # but its only 2 seconds
        time.sleep(nodeWaitTime)
        # make a client (connection) to started MongoDB Instance
        self.mongoClient = motor.motor_asyncio. \
            AsyncIOMotorClient(self.host, self.port)

        # Bootstrap db and tables
        self._bootstrapDB()

    def stop(self):
        try:
            call(["mongod", "--shutdown",
                  "--dbpath", "{}".format(self.dirpath)])
        except Exception as ex:
            logger.warn("Mongodb already stopped.")

    async def reset(self):
        await self.mongoClient.drop_database(self.dbName)

    def _bootstrapDB(self):
        try:
            # Get or create a DB
            self.db = self.mongoClient[self.dbName]
            # Get or create tables
            self.processedReqTB = self.db["processedRequests"]
            self.txnTB = self.db["txn"]
        except Exception as ex:
            logger.error("error creating database and tables: {}".format(ex))

    async def append(self, identifier: str, reply: Reply, txnId: str):
        try:
            # add reply to txn table
            jsonReply = self._createReplyRecord(txnId, reply)
            txnInsertRes = await self.txnTB.insert(jsonReply)
            logger.info("result for inserting txn with txnId: {} is {}".
                        format(txnId, txnInsertRes))

            # add txnId to processed transaction table
            reqId = reply.reqId
            jsonProcessedReq = self._createProcessedReqRecord(identifier,
                                                              reqId,
                                                              txnId)
            processedReqResult = await self.processedReqTB.insert(
                jsonProcessedReq)

            logger.info(
                "result for inserting processed request with identifier: {} "
                "reqId: {} and txnId: {} is {}".
                    format(identifier, reqId, txnId, processedReqResult))

        except Exception as ex:
            logger.error("error inserting transaction for identifier {}, "
                         "reply {}, and txnId {}: {}".
                         format(identifier, reply, txnId, ex))

    async def get(self, identifier: str, reqId: int):
        try:
            # Get processedReq from identifier and reqId
            processedReqQuery = {"identifier": identifier, "reqId": reqId}
            processedReq = await self.processedReqTB.find_one(processedReqQuery)

            # Get
            if processedReq:
                txnId = processedReq["txnId"]
                replyQuery = {"_id": txnId}
                jsonReply = await self.txnTB.find_one(replyQuery)
                return self._fromMongoJsonReply(jsonReply)
            else:
                return None
        except Exception as ex:
            logger.error("error getting transaction from DB {} and table "
                         "{} for identifier {} and reqId {}: {}".
                         format(self.dbName, self.processedReqTB,
                                identifier, reqId, ex))

    def _createReplyRecord(self, txnId, reply: Reply):
        return {
            "_id": txnId,
            "viewNo": reply.viewNo,
            "reqId": reply.reqId,
            "result": reply.result}

    def _createProcessedReqRecord(self, identifier, reqId, txnId):
        return {
            f.IDENTIFIER.nm: identifier,
            f.REQ_ID.nm: reqId,
            f.TXN_ID.nm: txnId
        }

    def _fromMongoJsonReply(self, jsonReply):
        return Reply(jsonReply[f.VIEW_NO.nm],
                     jsonReply[f.REQ_ID.nm],
                     jsonReply[f.RESULT.nm])

    async def size(self):
        return await self.txnTB.count()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
