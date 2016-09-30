import os

from ledger.serializers.compact_serializer import CompactSerializer
from ledger.stores.text_file_store import TextFileStore
from plenum.common.has_file_storage import HasFileStorage
from plenum.common.txn_util import getTxnOrderedFields
from plenum.common.util import updateFieldsWithSeqNo


class ClientTxnLog(HasFileStorage):
    """
    An immutable log of transactions made by the client.
    """
    def __init__(self, name, baseDir=None):
        self.dataDir = "data/clients"
        self.name = name
        HasFileStorage.__init__(self, name,
                                baseDir=baseDir,
                                dataDir=self.dataDir)
        self.clientDataLocation = self.dataLocation
        if not os.path.exists(self.clientDataLocation):
            os.makedirs(self.clientDataLocation)
        self.transactionLog = TextFileStore(self.clientDataLocation,
                                            "transactions")
        self.serializer = CompactSerializer(fields=self.txnFieldOrdering)

    @property
    def txnFieldOrdering(self):
        fields = getTxnOrderedFields()
        return updateFieldsWithSeqNo(fields)

    def append(self, reqId, txn):
        self.transactionLog.put(key=str(reqId), value=self.serializer.serialize(txn,
                                fields=self.txnFieldOrdering, toBytes=False))

    def hasTxnWithReqId(self, reqId) -> bool:
        for key in self.transactionLog.iterator(includeKey=True,
                                                includeValue=False):
            if key == str(reqId):
                return True
        return False
