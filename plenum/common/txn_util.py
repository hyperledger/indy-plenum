from collections import OrderedDict

from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.ledger import Ledger
from ledger.serializers.compact_serializer import CompactSerializer
from plenum.common.txn import TXN_ID, TXN_TIME, TXN_TYPE, TARGET_NYM, ROLE, \
    ALIAS
from plenum.common.types import f


def getTxnOrderedFields():
    return OrderedDict([
        (f.IDENTIFIER.nm, (str, str)),
        (f.REQ_ID.nm, (str, int)),
        (TXN_ID, (str, str)),
        (TXN_TIME, (str, float)),
        (TXN_TYPE, (str, str)),
        (TARGET_NYM, (str, str)),
        (ROLE, (str, str)),
        (ALIAS, (str, str))
    ])


def createGenesisTxnFile(genesisTxns, targetDir, fileName, fieldOrdering,
                         reset=True):
    ledger = Ledger(CompactMerkleTree(), dataDir=targetDir,
                    serializer=CompactSerializer(fields=fieldOrdering),
                    fileName=fileName)

    if reset:
        ledger.reset()

    reqIds = {}
    for txn in genesisTxns:
        identifier = txn.get(f.IDENTIFIER.nm, "")
        if identifier not in reqIds:
            reqIds[identifier] = 0
        reqIds[identifier] += 1
        txn.update({
            f.REQ_ID.nm: reqIds[identifier],
            f.IDENTIFIER.nm: identifier
        })
        ledger.add(txn)
    ledger.stop()
