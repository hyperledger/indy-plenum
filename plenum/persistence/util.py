
from copy import deepcopy

from ledger.util import F
from plenum.common.util import pop_keys


def txnsWithSeqNo(seqNoStart, seqNoEnd, txns):
    """
    Update each transaction with a sequence number field
    """
    txns = deepcopy(txns)
    for txn, seqNo in zip(txns, range(seqNoStart, seqNoEnd + 1)):
        txn[F.seqNo.name] = seqNo
    return txns


def txnsWithMerkleInfo(ledger, committedTxns):
    """
    Update each transaction with the merkle root hash and audit path
    """
    committedTxns = deepcopy(committedTxns)
    for txn in committedTxns:
        mi = ledger.merkleInfo(txn.get(F.seqNo.name))
        txn.update(mi)
    return committedTxns


def pop_merkle_info(txn):
    pop_keys(txn, lambda k: k in (F.auditPath.name, F.rootHash.name))
