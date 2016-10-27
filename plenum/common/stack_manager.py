import os
import shutil
from abc import abstractproperty
from collections import OrderedDict

from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.ledger import Ledger
from ledger.stores.file_hash_store import FileHashStore
from plenum.common.exceptions import RemoteNotFound
from plenum.common.raet import initRemoteKeep
from plenum.common.txn import DATA, ALIAS, TARGET_NYM, NODE_IP, CLIENT_IP, \
    CLIENT_PORT, NODE_PORT, VERKEY, TXN_TYPE, NEW_NODE, CHANGE_KEYS, CHANGE_HA
from plenum.common.types import HA, CLIENT_STACK_SUFFIX
from plenum.common.util import cryptonymToHex
from plenum.common.log import getlogger

logger = getlogger()


class TxnStackManager:
    def __init__(self, name, basedirpath, isNode=True):
        self.name = name
        self.basedirpath = basedirpath
        self.isNode = isNode

    @abstractproperty
    def hasLedger(self) -> bool:
        raise NotImplementedError

    @abstractproperty
    def ledgerLocation(self) -> str:
        raise NotImplementedError

    @abstractproperty
    def ledgerFile(self) -> str:
        raise NotImplementedError

    # noinspection PyTypeChecker
    @property
    def ledger(self):
        if self._ledger is None:
            if not self.hasLedger:
                defaultTxnFile = os.path.join(self.basedirpath,
                                              self.ledgerFile)
                if not os.path.isfile(defaultTxnFile):
                    raise FileNotFoundError("Pool transactions file not found")
                else:
                    shutil.copy(defaultTxnFile, self.ledgerLocation)

            dataDir = self.ledgerLocation
            self._ledger = Ledger(CompactMerkleTree(hashStore=FileHashStore(
                dataDir=dataDir)),
                dataDir=dataDir,
                fileName=self.ledgerFile)
        return self._ledger

    @staticmethod
    def parseLedgerForHaAndKeys(ledger):
        nodeReg = OrderedDict()
        cliNodeReg = OrderedDict()
        nodeKeys = {}
        for _, txn in ledger.getAllTxn().items():
            if txn[TXN_TYPE] in (NEW_NODE, CHANGE_KEYS, CHANGE_HA):
                nodeName = txn[DATA][ALIAS]
                nHa = (txn[DATA][NODE_IP], txn[DATA][NODE_PORT]) \
                    if (NODE_IP in txn[DATA] and NODE_PORT in txn[DATA]) \
                    else None
                cHa = (txn[DATA][CLIENT_IP], txn[DATA][CLIENT_PORT]) \
                    if (CLIENT_IP in txn[DATA] and CLIENT_PORT in txn[DATA]) \
                    else None
                if nHa:
                    nodeReg[nodeName] = HA(*nHa)
                if cHa:
                    cliNodeReg[nodeName + CLIENT_STACK_SUFFIX] = HA(*cHa)
                verkey = cryptonymToHex(txn[TARGET_NYM])
                nodeKeys[nodeName] = verkey

        return nodeReg, cliNodeReg, nodeKeys

    def addNewRemoteAndConnect(self, txn, remoteName, nodeOrClientObj):
        verkey = cryptonymToHex(txn[TARGET_NYM])

        nodeHa = (txn[DATA][NODE_IP], txn[DATA][NODE_PORT])
        cliHa = (txn[DATA][CLIENT_IP], txn[DATA][CLIENT_PORT])

        try:
            # Override any keys found, reason being the scenario where
            # before this node comes to know about the other node, the other
            # node tries to connect to it.
            initRemoteKeep(self.name, remoteName, self.basedirpath, verkey,
                           override=True)
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

    def stackHaChanged(self, txn, remoteName, nodeOrClientObj):
        nodeHa = (txn[DATA][NODE_IP], txn[DATA][NODE_PORT])
        cliHa = (txn[DATA][CLIENT_IP], txn[DATA][CLIENT_PORT])
        # try:
        #     rid = nodeOrClientObj.nodestack.removeRemoteByName(remoteName)
        #     logger.debug(
        #         "{} removed remote {}".format(nodeOrClientObj, remoteName))
        # except RemoteNotFound as ex:
        #     logger.info(ex)
        #     rid = None
        rid = self.removeRemote(nodeOrClientObj.nodestack, remoteName)
        if self.isNode:
            nodeOrClientObj.nodeReg[remoteName] = HA(*nodeHa)
            nodeOrClientObj.cliNodeReg[remoteName + CLIENT_STACK_SUFFIX] = HA(*cliHa)
        else:
            nodeOrClientObj.nodeReg[remoteName] = HA(*cliHa)
        return rid

    def stackKeysChanged(self, txn, remoteName, nodeOrClientObj):
        logger.debug("{} clearing remote role data in keep of {}".
                     format(nodeOrClientObj.nodestack.name, remoteName))
        nodeOrClientObj.nodestack.keep.clearRemoteRoleData(remoteName)
        logger.debug(
            "{} removing remote {}".format(nodeOrClientObj, remoteName))
        # Removing remote so that the nodestack will attempt to connect
        # try:
        #     rid = nodeOrClientObj.nodestack.removeRemoteByName(remoteName)
        #     logger.debug(
        #         "{} removed remote {}".format(nodeOrClientObj, remoteName))
        # except RemoteNotFound as ex:
        #     logger.info(ex)
        rid = self.removeRemote(nodeOrClientObj.nodestack, remoteName)

        verkey = txn[DATA][VERKEY]
        try:
            # Override any keys found
            initRemoteKeep(self.name, remoteName, self.basedirpath, verkey,
                           override=True)
        except Exception as ex:
            logger.error("Exception while initializing keep for remote {}".
                         format(ex))
        return rid

    @staticmethod
    def removeRemote(stack, remoteName):
        try:
            rid = stack.removeRemoteByName(remoteName)
            logger.debug(
                "{} removed remote {}".format(stack, remoteName))
        except RemoteNotFound as ex:
            logger.info(ex)
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
                if self.isNode:
                    initRemoteKeep(self.name, remoteName, self.basedirpath, key,
                                   override=True)
            except Exception as ex:
                logger.error("Exception while initializing keep for remote {}".
                             format(ex))

