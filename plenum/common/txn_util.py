import os
from collections import OrderedDict

import portalocker
from ledger.stores.file_hash_store import FileHashStore

from ledger.util import F

from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.ledger import Ledger
from ledger.serializers.compact_serializer import CompactSerializer
from plenum.common.txn import TXN_ID, TXN_TIME, TXN_TYPE, TARGET_NYM, ROLE, \
    ALIAS, VERKEY, TYPE, IDENTIFIER, DATA
from plenum.common.types import f
from plenum.common.log import getlogger


logger = getlogger()


def getTxnOrderedFields():
    return OrderedDict([
        (f.IDENTIFIER.nm, (str, str)),
        (f.REQ_ID.nm, (str, int)),
        (TXN_ID, (str, str)),
        (TXN_TIME, (str, int)),
        (TXN_TYPE, (str, str)),
        (TARGET_NYM, (str, str)),
        (VERKEY, (str, str)),
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


def updateGenesisPoolTxnFile(genesisTxnDir, genesisTxnFile, txn):
    # The lock is an advisory lock, it might not work on linux filesystems
    # not mounted with option `-o mand`, another approach can be to use a .lock
    # file to indicate presence or absence of .lock
    try:
        # Exclusively lock file in a non blocking manner. Locking is neccessary
        # since there might be multiple clients running on a machine so genesis
        #  files should be updated safely.
        # TODO: There is no automated test in the codebase that confirms it.
        # It has only been manaully tested in the python terminal. Add a test
        # for it using multiple processes writing concurrently
        with portalocker.Lock(os.path.join(genesisTxnDir, genesisTxnFile),
                              truncate=None,
                              flags=portalocker.LOCK_EX | portalocker.LOCK_NB):
            seqNo = txn[F.seqNo.name]
            ledger = Ledger(CompactMerkleTree(hashStore=FileHashStore(
                dataDir=genesisTxnDir)), dataDir=genesisTxnDir,
                fileName=genesisTxnFile)
            ledgerSize = len(ledger)
            if seqNo - ledgerSize == 1:
                ledger.add({k:v for k,v in txn.items() if k != F.seqNo.name})
                logger.debug('Adding transaction with sequence number {} in'
                             ' genesis pool transaction file'.format(seqNo))
            else:
                logger.debug('Already {} genesis pool transactions present so '
                             'transaction with sequence number {} '
                             'not applicable'.format(ledgerSize, seqNo))
    except (portalocker.LockException,
            portalocker.LockException) as ex:
        return
