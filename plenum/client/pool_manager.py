import collections
import json

from ledger.util import F
from plenum.common.stack_manager import TxnStackManager
from plenum.common.txn import TXN_TYPE, NEW_NODE, ALIAS, DATA, CHANGE_HA, \
    CHANGE_KEYS
from plenum.common.types import CLIENT_STACK_SUFFIX, PoolLedgerTxns, f
from plenum.common.util import getMaxFailures
from plenum.common.log import getlogger

logger = getlogger()
t = f.TXN.nm


class HasPoolManager(TxnStackManager):
    # noinspection PyUnresolvedReferences
    def __init__(self):
        self._ledgerFile = None
        self._ledgerLocation = None
        TxnStackManager.__init__(self, self.name, self.basedirpath,
                                 isNode=False)
        _, cliNodeReg, nodeKeys = self.parseLedgerForHaAndKeys(self.ledger)
        self.nodeReg = cliNodeReg
        self.addRemoteKeysFromLedger(nodeKeys)
        # Temporary place for keeping node transactions while this client is
        # discovering. Key is sequence number and value is dictionary where key
        # is the name of the node and value is the transaction. Transaction
        # should be applied in increasing order of sequence numbers.
        self.tempNodeTxns = {}  # type: Dict[int, Dict[str, Dict]]

    def poolTxnReceived(self, msg: PoolLedgerTxns, frm):
        logger.debug("{} received pool txn {} from {}".format(self, msg, frm))
        txn = getattr(msg, t)
        seqNo = txn.pop(F.seqNo.name)
        if seqNo not in self.tempNodeTxns:
            self.tempNodeTxns[seqNo] = {}
        self.tempNodeTxns[seqNo][frm] = txn
        # If this is the next sequence number that should go into ledger, then
        # check if there are enough same transactions from different nodes
        if (seqNo - self.ledger.size) == 1:
            f = getMaxFailures(len(self.nodeReg))
            if len(self.tempNodeTxns[seqNo]) > f:
                # TODO: Shouldnt this use `checkIfMoreThanFSameItems`
                txns = [item for item, count in
                        collections.Counter(
                            [json.dumps(t, sort_keys=True)
                             for t in self.tempNodeTxns[seqNo].values()]
                        ).items() if count > f]
                if len(txns) > 0:
                    self.addToLedger(json.loads(txns[0]))
                    self.tempNodeTxns.pop(seqNo)
                else:
                    logger.error("{} has not got enough similar node "
                                 "transactions".format(self))

    # noinspection PyUnresolvedReferences
    def processPoolTxn(self, txn):
        logger.debug("{} processing pool txn {} ".format(self, txn))
        typ = txn[TXN_TYPE]
        if typ == NEW_NODE:
            remoteName = txn[DATA][ALIAS] + CLIENT_STACK_SUFFIX
            self.addNewRemoteAndConnect(txn, remoteName, self)
            self.setF()
        elif typ == CHANGE_HA:
            remoteName = txn[DATA][ALIAS] + CLIENT_STACK_SUFFIX
            self.stackHaChanged(txn, remoteName, self)
        elif typ == CHANGE_KEYS:
            remoteName = txn[DATA][ALIAS] + CLIENT_STACK_SUFFIX
            self.stackKeysChanged(txn, remoteName, self)
        else:
            logger.error("{} received unknown txn type {} in txn {}"
                         .format(self.name, typ, txn))
            pass

    # noinspection PyUnresolvedReferences
    @property
    def hasLedger(self):
        return self.hasFile(self.ledgerFile)

    # noinspection PyUnresolvedReferences
    @property
    def ledgerLocation(self):
        if not self._ledgerLocation:
            self._ledgerLocation = self.dataLocation
        return self._ledgerLocation

    # noinspection PyUnresolvedReferences
    @property
    def ledgerFile(self):
        if not self._ledgerFile:
            self._ledgerFile = self.config.poolTransactionsFile
        return self._ledgerFile

    def addToLedger(self, txn):
        logger.debug("{} adding txn {} to pool ledger".format(self, txn))
        self.ledger.append(txn)
        self.processPoolTxn(txn)
