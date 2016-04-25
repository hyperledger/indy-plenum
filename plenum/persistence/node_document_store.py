import json

from plenum.common.types import f
from plenum.common.util import getlogger
from plenum.common.txn import TXN_ID, TXN_DATA

logger = getlogger()


class NodeDocumentStore:
    def __init__(self, store):
        self.store = store
        self.client = store.client
        self.bootstrap()

    def classesNeeded(self):
        return [
            (TXN_DATA, self.createTxnDataClass),
        ]

    def bootstrap(self):
        self.store.createClasses(self.classesNeeded())

    def createTxnDataClass(self):
        self.client.command("create class {}".format(TXN_DATA))
        self.store.createClassProperties(TXN_DATA, {
            "clientId": "string",
            "reqId": "long",
            TXN_ID: "string",
            "reply": "string",
            "serialNo": "long",
            "STH": "string",
            "auditPath": "embeddedlist string"
        })

        # Index in case we need to access all transactions of a client
        # self.createIndexOnClass(TXN_REQ, "clientId")

        self.client.command("create index CliReq on {} (clientId, reqId) unique".
                            format(TXN_DATA))
        self.store.createUniqueIndexOnClass(TXN_DATA, TXN_ID)
        self.store.createUniqueIndexOnClass(TXN_DATA, "serialNo")

    def addReplyForTxn(self, txnId, reply, clientId, reqId, serialNo, STH,
                       auditInfo):
        auditInfo = ", ".join(["'{}'".format(h) for h in auditInfo])
        self.client.command("update {} set {} = '{}', reply = '{}', "
                            "clientId = '{}', {} = {}, serialNo = {}, STH = '{}'"
                            ", auditPath = [{}] upsert where {} = '{}'".
                            format(TXN_DATA, TXN_ID, txnId, json.dumps(reply),
                                   clientId, f.REQ_ID.nm, reqId, serialNo,
                                   json.dumps(STH), auditInfo, TXN_ID, txnId))

    def getReply(self, identifier, reqId):
        result = self.client.command("select reply from {} where clientId = '{}'"
                                     " and reqId = {}".
                                     format(TXN_DATA, identifier, reqId))
        return None if not result else json.loads(result[0].oRecordData['reply'])

    def getRepliesForTxnIds(self, *txnIds, serialNo=None):
        txnIds = ",".join(["'{}'".format(tid) for tid in txnIds])
        cmd = "select serialNo, reply from {} where {} in [{}]".\
            format(TXN_DATA, TXN_ID, txnIds)
        if serialNo:
            cmd += " and serialNo > {}".format(serialNo)
        result = self.client.command(cmd)
        return {r.oRecordData["serialNo"]: json.loads(r.oRecordData["reply"])[2]
                for r in result}
