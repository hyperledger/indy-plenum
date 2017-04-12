import os

from copy import deepcopy

from ledger.util import F


def removeLockFiles(dbPath):
    if os.path.isdir(dbPath):
        lockFilePath = os.path.join(dbPath, 'LOCK')
        if os.path.isfile(lockFilePath):
            os.remove(lockFilePath)


def txnsWithSeqNo(seqNoStart, seqNoEnd, txns):
    txns = deepcopy(txns)
    for txn, seqNo in zip(txns, range(seqNoStart, seqNoEnd + 1)):
        txn[F.seqNo.name] = seqNo
    return txns


def txnsWithMerkleInfo(ledger, committedTxns):
    committedTxns = deepcopy(committedTxns)
    for txn in committedTxns:
        mi = ledger.merkleInfo(txn.get(F.seqNo.name))
        txn.update(mi)
    return committedTxns
