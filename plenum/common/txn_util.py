import os
from collections import OrderedDict

import portalocker

from ledger.stores.file_hash_store import FileHashStore

from ledger.util import F

from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.ledger import Ledger
from ledger.serializers.compact_serializer import CompactSerializer
from plenum.common.constants import TXN_TIME, TXN_TYPE, TARGET_NYM, ROLE, \
    ALIAS, VERKEY
from plenum.common.types import f, OPERATION
from plenum.common.request import Request
from stp_core.common.log import getlogger


logger = getlogger()


def getTxnOrderedFields():
    return OrderedDict([
        (f.IDENTIFIER.nm, (str, str)),
        (f.REQ_ID.nm, (str, int)),
        (f.SIG.nm, (str, str)),
        (TXN_TIME, (str, float)),
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
    genesisFilePath = os.path.join(genesisTxnDir, genesisTxnFile)
    try:
        # Exclusively lock file in a non blocking manner. Locking is neccessary
        # since there might be multiple clients running on a machine so genesis
        #  files should be updated safely.
        # TODO: There is no automated test in the codebase that confirms it.
        # It has only been manually tested in the python terminal. Add a test
        # for it using multiple processes writing concurrently
        with portalocker.Lock(genesisFilePath,
                              truncate=None,
                              flags=portalocker.LOCK_EX | portalocker.LOCK_NB):
            seqNo = txn[F.seqNo.name]
            fileHashStore = FileHashStore(dataDir=genesisTxnDir)
            ledger = Ledger(CompactMerkleTree(hashStore=fileHashStore),
                            dataDir=genesisTxnDir, fileName=genesisTxnFile)
            ledgerSize = len(ledger)
            if seqNo - ledgerSize == 1:
                ledger.add({k:v for k,v in txn.items() if k != F.seqNo.name})
                logger.debug('Adding transaction with sequence number {} in'
                             ' genesis pool transaction file'.format(seqNo))
            else:
                logger.debug('Already {} genesis pool transactions present so '
                             'transaction with sequence number {} '
                             'not applicable'.format(ledgerSize, seqNo))
            ledger.stop()
            fileHashStore.close()
    except portalocker.LockException as ex:
        logger.error("error occurred during locking file {}: {}".
                     format(genesisFilePath, str(ex)))


def reqToTxn(req: Request):
    """
    Transform a client request such that it can be stored in the ledger.
    Also this is what will be returned to the client in the reply
    :param req:
    :return:
    """
    data = req.signingState
    res = {
        f.IDENTIFIER.nm: req.identifier,
        f.REQ_ID.nm: req.reqId,
        f.SIG.nm: req.signature
    }
    res.update(data[OPERATION])
    return res
