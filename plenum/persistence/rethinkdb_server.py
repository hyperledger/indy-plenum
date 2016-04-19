import os
import signal
import time
from multiprocessing import Process, current_process
from subprocess import call

import rethinkdb as r
from rethinkdb.errors import ReqlRuntimeError, RqlError, ReqlError

from plenum.common.types import Reply, f
from plenum.common.util import checkPortAvailable, getlogger
from plenum.persistence.storage import Storage

logger = getlogger()


class RethinkDB(Storage):
    def __init__(self, host, port, dirpath: str):
        if not checkPortAvailable((host, port)):
            raise RuntimeError(
                "Address {}:{} already in use".format(host, port))
        self.host = host
        self.port = port
        self.portOffset = port - 28015
        self.driverPort = self.port + self.portOffset
        self.dbName = "db{}".format(port)
        self.processedReqTB = "processedRequests"
        self.txnTB = "txn"
        self.dirpath = dirpath

    def start(self, loop):
        def startdb():
            DB_CMD = 'rethinkdb'
            try:
                call([DB_CMD,
                      '--directory',
                      '{}'.format(self.dirpath),
                      '--driver-port',
                      '{}'.format(self.driverPort),
                      '--cluster-port',
                      '{}'.format(self.driverPort + 1000),
                      '--http-port',
                      '{}'.format(8080 + self.portOffset)
                      ])
            except Exception as ex:
                raise RuntimeError("Could not call '{}'; is it installed?".
                                   format(DB_CMD)) from ex

        p = Process(target=startdb)
        p.start()
        time.sleep(15)  # todo: FIX THIS we cannot have blocking code!!!!
        with r.connect(host=self.host, port=self.driverPort) as rdbConn:
            if self.dbName not in r.db_list().run(rdbConn):
                self._bootstrapDB()

    def stop(self):
        try:
            with r.connect(host=self.host, port=self.driverPort) as rdbConn:
                for status in list(
                        r.db('rethinkdb').table('server_status').run(rdbConn)):
                    pid = int(status['process']['pid'])
                    call(['kill', '-9', '{}'.format(pid)])
        except Exception as ex:
            logger.warn("RethinkDB already stopped.")

    def reset(self):
        with r.connect(host=self.host, port=self.driverPort) as rdbConn:
            r.db_drop(self.dbName).run(rdbConn)

    def _bootstrapDB(self):
        self._createDB(self.dbName)
        self._createTable(self.dbName, self.processedReqTB)
        self._createTable(self.dbName, self.txnTB)

    # this async does not make it async,
    # it is just in convention with storage intrface
    async def append(self, identifier: str, reply: Reply, txnId: str):
        with r.connect(host=self.host, port=self.driverPort) as rdbConn:
            try:
                # add txn to table
                reply = self._createReplyRecord(txnId, reply)
                r.db(self.dbName).table(self.txnTB).insert(reply).run(rdbConn)

                # add to processed transaction
                processedReq = self._createProcessedReqRecord(identifier,
                                                              reply["reqId"],
                                                              txnId)
                r.db(self.dbName).table(self.processedReqTB).insert(
                    processedReq).run(rdbConn)
            except ReqlError as ex:
                logger.error("error inserting transaction for identifier {}, "
                             "reply {}, and txnId {}: {}".
                             format(identifier, reply, txnId, ex))

    async def get(self, identifier, reqId):
        with r.connect(host=self.host, port=self.driverPort) as rdbConn:
            try:
                key = str(identifier) + "-" + str(reqId)
                processedRecord = r.db(self.dbName). \
                    table(self.processedReqTB). \
                    get(key). \
                    run(rdbConn)
                if processedRecord:
                    jsonReply = r.db(self.dbName).table(self.txnTB).get(
                        processedRecord[f.TXN_ID.nm]).run(rdbConn)
                    return self._replyFromJson(jsonReply)
                else:
                    return None
            except ReqlError as ex:
                logger.error("error getting transaction from DB {} and table "
                             "{} for identifier {} and reqId {}: {}".
                             format(self.dbName, self.processedReqTB,
                                    identifier, reqId, ex))

    def _createDB(self, dbName):
        with r.connect(host=self.host, port=self.driverPort) as rdbConn:
            try:
                r.db_create(dbName).run(rdbConn)
            except RqlError as ex:
                logger.error(str(ex))

    def _createTable(self, dbName, tableName):
        with r.connect(host=self.host, port=self.driverPort) as rdbConn:
            try:
                if dbName and tableName:
                    r.db(dbName).table_create(tableName).run(rdbConn)
                else:
                    raise RuntimeError("DB name or Table name is empty")
            except ReqlRuntimeError:
                logger.error("table: {} already exists on "
                             "database: {}".format(tableName, dbName))

    def _createReplyRecord(self, txnId, reply: Reply):
        return {
            "id": txnId,
            "viewNo": reply.viewNo,
            "reqId": reply.reqId,
            "result": reply.result}

    def _createProcessedReqRecord(self, identifier, reqId, txnId):
        return {
            "id": str(identifier) + "-" + str(reqId),
            "txnId": txnId
        }

    def _createResponseRecord(self, identifier, response: Reply):
        return {
            "id": identifier,
            "responses": [self._replyToJson(response)]
        }

    def _replyToJson(self, reply):
        return {
            "viewNo": reply.viewNo,
            "reqId": reply.reqId,
            "result": reply.result
        }

    def _replyFromJson(self, jsonReply):
        return Reply(jsonReply["viewNo"], jsonReply["reqId"],
                     jsonReply["result"])

    async def size(self):
        with r.connect(host=self.host, port=self.driverPort) as rdbConn:
            return r.db(self.dbName).table(self.txnTB).count().run(rdbConn)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
