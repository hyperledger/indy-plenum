import json
from typing import Any

from plenum.common.types import f
from plenum.common.request import Request

from plenum.common.txn import TXN_ID, TXN_TIME, TXN_TYPE
from plenum.common.util import getlogger

logger = getlogger()

REQ_DATA = "ReqData"
ATTR_DATA = "AttrData"
LAST_TXN_DATA = "LastTxnData"


# TODO Rename it to its proper domain name.
# class ClientDocumentStore:
#     def __init__(self, store):
#         self.store = store
#         self.client = store.client
#         self.bootstrap()
#
#     def classesNeeded(self):
#         raise NotImplementedError
#
#     def bootstrap(self):
#         self.store.createClasses(self.classesNeeded())
#
#     def createAttributeClass(self):
#         raise NotImplementedError
#
#     def createReqDataClass(self):
#         raise NotImplementedError
#
#     def createLastTxnClass(self):
#         self.client.command("create class {}".format(LAST_TXN_DATA))
#         self.store.createClassProperties(LAST_TXN_DATA, {
#             f.IDENTIFIER.nm: "string",
#             "value": "string",
#         })
#         self.store.createUniqueIndexOnClass(LAST_TXN_DATA, f.IDENTIFIER.nm)
#
#     def getLastReqId(self):
#         result = self.client.command("select max({}) as lastId from {}".
#                                      format(f.REQ_ID.nm, REQ_DATA))
#         return 0 if not result else result[0].oRecordData['lastId']
#
#     def addRequest(self, req: Request):
#         self.client.command("insert into {} set {} = {}, {} = '{}', {} = '{}', "
#                             "nacks = {}, replies = {}".
#                             format(REQ_DATA, f.REQ_ID.nm, req.reqId,
#                                    f.IDENTIFIER.nm, req.identifier, TXN_TYPE,
#                                    req.operation[TXN_TYPE], "{}", "{}"))
#
#     def addAck(self, msg: Any, sender: str):
#         reqId = msg[f.REQ_ID.nm]
#         self.client.command("update {} add acks = '{}' where {} = {}".
#                             format(REQ_DATA, sender, f.REQ_ID.nm, reqId))
#
#     def addNack(self, msg: Any, sender: str):
#         reqId = msg[f.REQ_ID.nm]
#         reason = msg[f.REASON.nm]
#         reason = reason.replace('"', '\\"').replace("'", "\\'")
#         self.client.command("update {} set nacks.{} = '{}' where {} = {}".
#                             format(REQ_DATA, sender, reason, f.REQ_ID.nm, reqId))
#
#     # noinspection PyUnresolvedReferences
#     def addReply(self, reqId: int, sender: str, result: Any):
#         txnId = result[TXN_ID]
#         txnTime = result[TXN_TIME]
#         serializedTxn = self._serializeTxn(result)
#         res = self.client.command("update {} set replies.{} = '{}' return "
#                                   "after @this.replies where {} = {}".
#                             format(REQ_DATA, sender, serializedTxn,
#                                    f.REQ_ID.nm, reqId))
#         replies = res[0].oRecordData['value']
#         # TODO: Handle malicious nodes sending incorrect response
#         # If first reply for the request then update txn id and txn time
#         if len(replies) == 1:
#             self.client.command("update {} set {} = '{}', {} = {}, {} = '{}' "
#                                 "where {} = {}".
#                                 format(REQ_DATA, TXN_ID, txnId, TXN_TIME,
#                                        txnTime, TXN_TYPE, result[TXN_TYPE],
#                                        f.REQ_ID.nm, reqId))
#         return replies
#
#     def addAttribute(self, reqId, attrData):
#         data = json.dumps(attrData)
#         result = self.client.command("insert into {} content {}".
#                                      format(ATTR_DATA, data))
#         self.client.command("update {} set attribute = {} where {} = {}".
#                             format(REQ_DATA, result[0].oRecordData,
#                                    f.REQ_ID.nm, reqId))
#
#     def hasRequest(self, reqId: int):
#         result = self.client.command("select from {} where {} = {}".
#                                      format(REQ_DATA, f.REQ_ID.nm, reqId))
#         return bool(result)
#
#     # noinspection PyUnresolvedReferences
#     def getReplies(self, reqId: int):
#         result = self.client.command("select replies from {} where {} = {}".
#                                      format(REQ_DATA, f.REQ_ID.nm, reqId))
#         if not result:
#             return {}
#         else:
#             return {
#                 k: self.serializer.deserialize(v, fields=self.txnFields)
#                 for k, v in result[0].oRecordData['replies'].items()
#             }
#
#     def getAcks(self, reqId: int):
#         # Returning a dictionary here just for consistency
#         result = self.client.command("select acks from {} where {} = {}".
#                                      format(REQ_DATA, f.REQ_ID.nm, reqId))
#         if not result:
#             return {}
#         result = {k: 1 for k in result[0].oRecordData.get('acks', [])}
#         return result
#
#     def getNacks(self, reqId: int):
#         result = self.client.command("select nacks from {} where {} = {}".
#                                      format(REQ_DATA, f.REQ_ID.nm, reqId))
#         return {} if not result else result[0].oRecordData.get('nacks', {})
#
#     def getAllReplies(self, reqId):
#         replies = self.getReplies(reqId)
#         errors = self.getNacks(reqId)
#         return replies, errors
#
#     def requestHasConsensus(self, reqId):
#         self.client.command("update {} set hasConsensus = true where {} = {}".
#                             format(REQ_DATA, f.REQ_ID.nm, reqId))
#
#     def getRepliesByTxnId(self, txnId):
#         result = self.client.command("select replies from {} where {} = '{}'".
#                                      format(REQ_DATA, TXN_ID, txnId))
#         return [] if not result else result[0].oRecordData['replies']
#
#     def getRepliesByTxnType(self, txnType):
#         result = self.client.command("select replies from {} where {} = '{}'".
#                                      format(REQ_DATA, TXN_TYPE, txnType))
#
#         return [] if not result else [r.oRecordData['replies'] for r in result]
#
#     def setLastTxnForIdentifier(self, identifier, value):
#         self.client.command("update {} set value = '{}', {} = '{}' upsert "
#                             "where {} = '{}'".
#                             format(LAST_TXN_DATA, value, f.IDENTIFIER.nm,
#                                    identifier, f.IDENTIFIER.nm, identifier))
#
#     def getValueForIdentifier(self, identifier):
#         result = self.client.command("select value from {} where {} = '{}'".
#                                      format(LAST_TXN_DATA, f.IDENTIFIER.nm,
#                                             identifier))
#         return None if not result else result[0].oRecordData['value']
