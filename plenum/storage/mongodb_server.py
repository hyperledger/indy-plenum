import os
from subprocess import call

from multiprocessing import Process

import time

import motor.motor_asyncio

from plenum.common.request_types import Reply
from plenum.common.util import getlogger, checkPortAvailable
from plenum.storage.storage import Storage

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

    async def insertTxn(self, clientId: str, reply: Reply, txnId: str):
        try:
            # add reply to txn table
            jsonReply = self._createReplyRecord(txnId, reply)
            txnInsertRes = await self.txnTB.insert(jsonReply)
            logger.info("result for inserting txn with txnId: {} is {}".
                        format(txnId, txnInsertRes))

            # add txnId to processed transaction table
            reqId = reply.reqId
            jsonProcessedReq = self._createProcessedReqRecord(clientId,
                                                              reqId,
                                                              txnId)
            processedReqResult = await self.processedReqTB.insert(
                jsonProcessedReq)

            logger.info(
                "result for inserting processed request with clientId: {} "
                "reqId: {} and txnId: {} is {}".
                    format(clientId, reqId, txnId, processedReqResult))

        except Exception as ex:
            logger.error("error inserting transaction for clientId {}, "
                         "reply {}, and txnId {}: {}".
                         format(clientId, reply, txnId, ex))

    async def getTxn(self, clientId: str, reqId: int):
        try:
            # Get processedReq from clientId and reqId
            processedReqQuery = {"clientId": clientId, "reqId": reqId}
            processedReq = await self.processedReqTB.find_one(processedReqQuery)

            # Get
            if processedReq:
                txnId = processedReq["txnId"]
                replyQuery = {"txnId": txnId}
                jsonReply = await self.txnTB.find_one(replyQuery)
                return self._fromMongoJsonReply(jsonReply)
            else:
                return None
        except Exception as ex:
            logger.error("error getting transaction from DB {} and table "
                         "{} for clientId {} and reqId {}: {}".
                         format(self.dbName, self.processedReqTB,
                                clientId, reqId, ex))

    def _createReplyRecord(self, txnId, reply: Reply):
        return {
            "txnId": txnId,
            "viewNo": reply.viewNo,
            "reqId": reply.reqId,
            "result": reply.result}

    def _createProcessedReqRecord(self, clientId, reqId, txnId):
        return {
            "clientId": clientId,
            "reqId": reqId,
            "txnId": txnId
        }

    def _fromMongoJsonReply(self, jsonReply):
        return Reply(jsonReply["viewNo"],
                     jsonReply["reqId"],
                     jsonReply["result"])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
