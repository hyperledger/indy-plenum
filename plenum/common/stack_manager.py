import os
import shutil
from abc import abstractmethod
from collections import OrderedDict
from typing import List

from plenum.common.keygen_utils import initRemoteKeys
from plenum.common.signer_did import DidIdentity
from stp_core.types import HA
from stp_core.network.exceptions import RemoteNotFound
from stp_core.common.log import getlogger
from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.stores.file_hash_store import FileHashStore

from plenum.common.constants import DATA, ALIAS, TARGET_NYM, NODE_IP, CLIENT_IP, \
    CLIENT_PORT, NODE_PORT, VERKEY, TXN_TYPE, NODE, SERVICES, VALIDATOR, CLIENT_STACK_SUFFIX
from plenum.common.util import cryptonymToHex, updateNestedDict
from plenum.common.ledger import Ledger

logger = getlogger()


class TxnStackManager:
    def __init__(self, name, basedirpath, isNode=True):
        self.name = name
        self.basedirpath = basedirpath
        self.isNode = isNode
        self.hashStore = None

    @property
    @abstractmethod
    def hasLedger(self) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def ledgerLocation(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def ledgerFile(self) -> str:
        raise NotImplementedError

    # noinspection PyTypeChecker
    @property
    def ledger(self):
        if self._ledger is None:
            defaultTxnFile = os.path.join(self.basedirpath,
                                          self.ledgerFile)
            if not os.path.exists(defaultTxnFile):
                logger.debug("Not using default initialization file for "
                             "pool ledger, since it does not exist: {}"
                             .format(defaultTxnFile))
                defaultTxnFile = None

            dataDir = self.ledgerLocation
            self.hashStore = FileHashStore(dataDir=dataDir)
            self._ledger = Ledger(CompactMerkleTree(hashStore=self.hashStore),
                                  dataDir=dataDir,
                                  fileName=self.ledgerFile,
                                  ensureDurability=self.config.EnsureLedgerDurability,
                                  defaultFile=defaultTxnFile)
        return self._ledger

    @staticmethod
    def parseLedgerForHaAndKeys(ledger, returnActive=True):
        """
        Returns validator ip, ports and keys
        :param ledger:
        :param returnActive: If returnActive is True, return only those
        validators which are not out of service
        :return:
        """
        nodeReg = OrderedDict()
        cliNodeReg = OrderedDict()
        nodeKeys = {}
        activeValidators = set()
        for _, txn in ledger.getAllTxn():
            if txn[TXN_TYPE] == NODE:
                nodeName = txn[DATA][ALIAS]
                clientStackName = nodeName + CLIENT_STACK_SUFFIX
                nHa = (txn[DATA][NODE_IP], txn[DATA][NODE_PORT]) \
                    if (NODE_IP in txn[DATA] and NODE_PORT in txn[DATA]) \
                    else None
                cHa = (txn[DATA][CLIENT_IP], txn[DATA][CLIENT_PORT]) \
                    if (CLIENT_IP in txn[DATA] and CLIENT_PORT in txn[DATA]) \
                    else None
                if nHa:
                    nodeReg[nodeName] = HA(*nHa)
                if cHa:
                    cliNodeReg[clientStackName] = HA(*cHa)

                try:
                    # TODO: Need to handle abbreviated verkey
                    verkey = cryptonymToHex(txn[TARGET_NYM])
                except ValueError as ex:
                    raise ValueError("Invalid verkey. Rebuild pool transactions.")

                nodeKeys[nodeName] = verkey

                services = txn[DATA].get(SERVICES)
                if isinstance(services, list):
                    if VALIDATOR in services:
                        activeValidators.add(nodeName)
                    else:
                        activeValidators.discard(nodeName)

        if returnActive:
            allNodes = tuple(nodeReg.keys())
            for nodeName in allNodes:
                if nodeName not in activeValidators:
                    nodeReg.pop(nodeName, None)
                    cliNodeReg.pop(nodeName + CLIENT_STACK_SUFFIX, None)
                    nodeKeys.pop(nodeName, None)

            return nodeReg, cliNodeReg, nodeKeys
        else:
            return nodeReg, cliNodeReg, nodeKeys, activeValidators

    def connectNewRemote(self, txn, remoteName, nodeOrClientObj,
                         addRemote=True):
        # TODO: Need to handle abbreviated verkey
        verkey = cryptonymToHex(txn[TARGET_NYM])

        nodeHa = (txn[DATA][NODE_IP], txn[DATA][NODE_PORT])
        cliHa = (txn[DATA][CLIENT_IP], txn[DATA][CLIENT_PORT])

        if addRemote:
            try:
                # Override any keys found, reason being the scenario where
                # before this node comes to know about the other node, the other
                # node tries to connect to it.
                initRemoteKeys(self.name, remoteName, self.basedirpath,
                                   verkey, override=True)
            except Exception as ex:
                logger.error("Exception while initializing keep for remote {}".
                             format(ex))

        if self.isNode:
            nodeOrClientObj.nodeReg[remoteName] = HA(*nodeHa)
            nodeOrClientObj.cliNodeReg[remoteName + CLIENT_STACK_SUFFIX] = HA(*cliHa)
            logger.debug("{} adding new node {} with HA {}".format(self.name,
                                                                   remoteName,
                                                                   nodeHa))
        else:
            nodeOrClientObj.nodeReg[remoteName] = HA(*cliHa)
            logger.debug("{} adding new node {} with HA {}".format(self.name,
                                                                   remoteName,
                                                                   cliHa))
        nodeOrClientObj.nodestack.maintainConnections(force=True)

    def stackHaChanged(self, txn, remoteName, nodeOrClientObj):
        nodeHa = (txn[DATA][NODE_IP], txn[DATA][NODE_PORT])
        cliHa = (txn[DATA][CLIENT_IP], txn[DATA][CLIENT_PORT])
        rid = self.removeRemote(nodeOrClientObj.nodestack, remoteName)
        if self.isNode:
            nodeOrClientObj.nodeReg[remoteName] = HA(*nodeHa)
            nodeOrClientObj.cliNodeReg[remoteName + CLIENT_STACK_SUFFIX] = HA(*cliHa)
        else:
            nodeOrClientObj.nodeReg[remoteName] = HA(*cliHa)

        # Attempt connection at the new HA
        nodeOrClientObj.nodestack.maintainConnections(force=True)

        return rid

    def stackKeysChanged(self, txn, remoteName, nodeOrClientObj):
        logger.debug("{} clearing remote role data in keep of {}".
                     format(nodeOrClientObj.nodestack.name, remoteName))
        logger.debug(
            "{} removing remote {}".format(nodeOrClientObj, remoteName))
        # Removing remote so that the nodestack will attempt to connect
        rid = self.removeRemote(nodeOrClientObj.nodestack, remoteName)

        if txn[VERKEY][0] == '~':  # abbreviated
            verkey = cryptonymToHex(txn[TARGET_NYM]) + cryptonymToHex(txn[VERKEY][1:])
        else:
            verkey = cryptonymToHex(txn[VERKEY])

        # Override any keys found
        initRemoteKeys(self.name, remoteName, self.basedirpath,
                               verkey, override=True)

        # Attempt connection with the new keys
        nodeOrClientObj.nodestack.maintainConnections(force=True)
        return rid

    @staticmethod
    def removeRemote(stack, remoteName):
        try:
            stack.disconnectByName(remoteName)
            rid = stack.removeRemoteByName(remoteName)
            logger.debug(
                "{} removed remote {}".format(stack, remoteName))
        except RemoteNotFound as ex:
            logger.debug(str(ex))
            rid = None

        return rid

    def addRemoteKeysFromLedger(self, keys):
        for remoteName, key in keys.items():
            # If its a client then remoteName should be suffixed with
            # CLIENT_STACK_SUFFIX
            if not self.isNode:
                remoteName += CLIENT_STACK_SUFFIX
            try:
                # Override any keys found, reason being the scenario where
                # before this node comes to know about the other node, the other
                # node tries to connect to it.
                # Do it only for Nodes, not for Clients!
                #if self.isNode:
                initRemoteKeys(self.name, remoteName, self.basedirpath, key,
                                   override=True)
            except Exception as ex:
                logger.error("Exception while initializing keep for remote {}".
                             format(ex))

    def nodeExistsInLedger(self, nym):
        # Since PoolLedger is going to be small so using
        # `getAllTxn` is fine
        for _, txn in self.ledger.getAllTxn():
            if txn[TXN_TYPE] == NODE and \
                            txn[TARGET_NYM] == nym:
                return True
        return False

    # TODO: Consider removing `nodeIds` and using `node_ids_in_order`
    @property
    def nodeIds(self) -> set:
        return {txn[TARGET_NYM] for _, txn in self.ledger.getAllTxn()}

    def getNodeInfoFromLedger(self, nym, excludeLast=True):
        # Returns the info of the node from the ledger with transaction
        # sequence numbers that added or updated the info excluding the last
        # update transaction. The reason for ignoring last transactions is that
        #  it is used after update to the ledger has already been made
        txns = []
        nodeTxnSeqNos = []
        for seqNo, txn in self.ledger.getAllTxn():
            if txn[TXN_TYPE] == NODE and txn[TARGET_NYM] == nym:
                txns.append(txn)
                nodeTxnSeqNos.append(seqNo)
        info = {}
        if len(txns) > 1 and excludeLast:
            txns = txns[:-1]
        for txn in txns:
            self.updateNodeTxns(info, txn)
        return nodeTxnSeqNos, info

    @staticmethod
    def updateNodeTxns(oldTxn, newTxn):
        updateNestedDict(oldTxn, newTxn, nestedKeysToUpdate=[DATA, ])

