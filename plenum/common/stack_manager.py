from abc import abstractmethod, ABCMeta
from collections import OrderedDict

from ledger.genesis_txn.genesis_txn_initiator_from_file import GenesisTxnInitiatorFromFile
from plenum.common.keygen_utils import initRemoteKeys
from plenum.common.tools import lazy_field
from plenum.common.txn_util import get_payload_data, get_type, get_from
from storage.helper import initHashStore
from stp_core.types import HA
from stp_core.network.exceptions import RemoteNotFound
from stp_core.common.log import getlogger
from ledger.compact_merkle_tree import CompactMerkleTree

from plenum.common.constants import DATA, ALIAS, TARGET_NYM, NODE_IP, CLIENT_IP, \
    CLIENT_PORT, NODE_PORT, VERKEY, NODE, SERVICES, VALIDATOR, CLIENT_STACK_SUFFIX
from plenum.common.util import cryptonymToHex, updateNestedDict
from plenum.common.ledger import Ledger

logger = getlogger()


class TxnStackManager(metaclass=ABCMeta):
    def __init__(self, name, genesis_dir, keys_dir, isNode=True):
        self.name = name
        self.genesis_dir = genesis_dir
        self.keys_dir = keys_dir
        self.isNode = isNode

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

    @lazy_field
    def hashStore(self):
        return initHashStore(self.ledgerLocation, 'pool', self.config)

    # noinspection PyTypeChecker
    @lazy_field
    def ledger(self):
        data_dir = self.ledgerLocation
        genesis_txn_initiator = GenesisTxnInitiatorFromFile(self.genesis_dir,
                                                            self.ledgerFile)
        tree = CompactMerkleTree(hashStore=self.hashStore)
        ledger = Ledger(tree,
                        dataDir=data_dir,
                        fileName=self.ledgerFile,
                        ensureDurability=self.config.EnsureLedgerDurability,
                        genesis_txn_initiator=genesis_txn_initiator)

        return ledger

    @staticmethod
    def parseLedgerForHaAndKeys(ledger, returnActive=True, ledger_size=None):
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
        try:
            TxnStackManager._parse_pool_transaction_file(
                ledger, nodeReg, cliNodeReg, nodeKeys, activeValidators,
                ledger_size=ledger_size)
        except ValueError:
            errMsg = 'Pool transaction file corrupted. Rebuild pool transactions.'
            logger.exception(errMsg)
            exit(errMsg)

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

    @staticmethod
    def _parse_pool_transaction_file(
            ledger, nodeReg, cliNodeReg, nodeKeys, activeValidators,
            ledger_size=None):
        """
        helper function for parseLedgerForHaAndKeys
        """
        for _, txn in ledger.getAllTxn(to=ledger_size):
            if get_type(txn) == NODE:
                txn_data = get_payload_data(txn)
                nodeName = txn_data[DATA][ALIAS]
                clientStackName = nodeName + CLIENT_STACK_SUFFIX
                nHa = (txn_data[DATA][NODE_IP], txn_data[DATA][NODE_PORT]) \
                    if (NODE_IP in txn_data[DATA] and NODE_PORT in txn_data[DATA]) \
                    else None
                cHa = (txn_data[DATA][CLIENT_IP], txn_data[DATA][CLIENT_PORT]) \
                    if (CLIENT_IP in txn_data[DATA] and CLIENT_PORT in txn_data[DATA]) \
                    else None
                if nHa:
                    nodeReg[nodeName] = HA(*nHa)
                if cHa:
                    cliNodeReg[clientStackName] = HA(*cHa)

                try:
                    # TODO: Need to handle abbreviated verkey
                    key_type = 'verkey'
                    verkey = cryptonymToHex(str(txn_data[TARGET_NYM]))
                    key_type = 'identifier'
                    cryptonymToHex(get_from(txn))
                except ValueError:
                    logger.exception(
                        'Invalid {}. Rebuild pool transactions.'.format(key_type))
                    exit('Invalid {}. Rebuild pool transactions.'.format(key_type))

                nodeKeys[nodeName] = verkey

                services = txn_data[DATA].get(SERVICES)
                if isinstance(services, list):
                    if VALIDATOR in services:
                        activeValidators.add(nodeName)
                    else:
                        activeValidators.discard(nodeName)

    def connectNewRemote(self, txn_data, remoteName, nodeOrClientObj,
                         addRemote=True):
        # TODO: Need to handle abbreviated verkey
        verkey = cryptonymToHex(txn_data[TARGET_NYM])

        nodeHa = (txn_data[DATA][NODE_IP], txn_data[DATA][NODE_PORT])
        cliHa = (txn_data[DATA][CLIENT_IP], txn_data[DATA][CLIENT_PORT])

        if addRemote:
            try:
                # Override any keys found, reason being the scenario where
                # before this node comes to know about the other node, the other
                # node tries to connect to it.
                initRemoteKeys(self.name, remoteName, self.keys_dir, verkey, override=True)
            except Exception as ex:
                logger.error("Exception while initializing keep for remote {}".
                             format(ex))

        if self.isNode:
            nodeOrClientObj.nodeReg[remoteName] = HA(*nodeHa)
            nodeOrClientObj.cliNodeReg[remoteName +
                                       CLIENT_STACK_SUFFIX] = HA(*cliHa)
            logger.display("{} adding new node {} with HA {}".format(self.name, remoteName, nodeHa))
        else:
            nodeOrClientObj.nodeReg[remoteName] = HA(*cliHa)
            logger.display("{} adding new node {} with HA {}".format(self.name, remoteName, cliHa))
        nodeOrClientObj.nodestack.maintainConnections(force=True)

    def stackHaChanged(self, txn_data, remoteName, nodeOrClientObj):
        nodeHa = None
        cliHa = None
        if self.isNode:
            node_ha_changed = False
            (ip, port) = nodeOrClientObj.nodeReg[remoteName]
            if NODE_IP in txn_data[DATA] and ip != txn_data[DATA][NODE_IP]:
                ip = txn_data[DATA][NODE_IP]
                node_ha_changed = True

            if NODE_PORT in txn_data[DATA] and port != txn_data[DATA][NODE_PORT]:
                port = txn_data[DATA][NODE_PORT]
                node_ha_changed = True

            if node_ha_changed:
                nodeHa = (ip, port)

        cli_ha_changed = False
        (ip, port) = nodeOrClientObj.cliNodeReg[remoteName + CLIENT_STACK_SUFFIX] \
            if self.isNode \
            else nodeOrClientObj.nodeReg[remoteName]

        if CLIENT_IP in txn_data[DATA] and ip != txn_data[DATA][CLIENT_IP]:
            ip = txn_data[DATA][CLIENT_IP]
            cli_ha_changed = True

        if CLIENT_PORT in txn_data[DATA] and port != txn_data[DATA][CLIENT_PORT]:
            port = txn_data[DATA][CLIENT_PORT]
            cli_ha_changed = True

        if cli_ha_changed:
            cliHa = (ip, port)

        rid = self.removeRemote(nodeOrClientObj.nodestack, remoteName)
        if self.isNode:
            if nodeHa:
                nodeOrClientObj.nodeReg[remoteName] = HA(*nodeHa)
            if cliHa:
                nodeOrClientObj.cliNodeReg[remoteName +
                                           CLIENT_STACK_SUFFIX] = HA(*cliHa)
        elif cliHa:
            nodeOrClientObj.nodeReg[remoteName] = HA(*cliHa)

        # Attempt connection at the new HA
        nodeOrClientObj.nodestack.maintainConnections(force=True)

        return rid

    def stackKeysChanged(self, txn_data, remoteName, nodeOrClientObj):
        logger.debug("{} clearing remote role data in keep of {}".
                     format(nodeOrClientObj.nodestack.name, remoteName))
        logger.display("{} removing remote {}".format(nodeOrClientObj, remoteName))
        # Removing remote so that the nodestack will attempt to connect
        rid = self.removeRemote(nodeOrClientObj.nodestack, remoteName)

        if txn_data[VERKEY][0] == '~':  # abbreviated
            verkey = cryptonymToHex(
                txn_data[TARGET_NYM]) + cryptonymToHex(txn_data[VERKEY][1:])
        else:
            verkey = cryptonymToHex(txn_data[VERKEY])

        # Override any keys found
        initRemoteKeys(self.name, remoteName, self.keys_dir, verkey, override=True)

        # Attempt connection with the new keys
        nodeOrClientObj.nodestack.maintainConnections(force=True)
        return rid

    @staticmethod
    def removeRemote(stack, remoteName):
        try:
            stack.disconnectByName(remoteName)
            rid = stack.removeRemoteByName(remoteName)
            logger.display("{} removed remote {}".format(stack, remoteName))
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
                # if self.isNode:
                initRemoteKeys(self.name, remoteName, self.keys_dir, key,
                               override=True)
            except Exception as ex:
                logger.error("Exception while initializing keep for remote {}".
                             format(ex))

    def getNodeRegistry(self, ledger_size=None):
        nodeReg, _, _ = self.parseLedgerForHaAndKeys(
            self.ledger, ledger_size=ledger_size)
        return nodeReg

    def nodeExistsInLedger(self, nym):
        # Since PoolLedger is going to be small so using
        # `getAllTxn` is fine
        for _, txn in self.ledger.getAllTxn():
            if get_type(txn) == NODE and \
                    get_payload_data(txn)[TARGET_NYM] == nym:
                return True
        return False

    # TODO: Consider removing `nodeIds` and using `node_ids_in_order`
    @property
    def nodeIds(self) -> set:
        return {get_payload_data(txn)[TARGET_NYM] for _, txn in self.ledger.getAllTxn()}

    def getNodeInfoFromLedger(self, nym, excludeLast=True):
        # Returns the info of the node from the ledger with transaction
        # sequence numbers that added or updated the info excluding the last
        # update transaction. The reason for ignoring last transactions is that
        #  it is used after update to the ledger has already been made
        txns = []
        nodeTxnSeqNos = []
        for seqNo, txn in self.ledger.getAllTxn():
            txn_data = get_payload_data(txn)
            if get_type(txn) == NODE and txn_data[TARGET_NYM] == nym:
                txns.append(txn)
                nodeTxnSeqNos.append(seqNo)
        info = {}
        if len(txns) > 1 and excludeLast:
            txns = txns[:-1]
        for txn in txns:
            self.updateNodeTxns(info, get_payload_data(txn))
        return nodeTxnSeqNos, info

    def getNodesServices(self):
        # Returns services for each node
        srvs = dict()
        for _, txn in self.ledger.getAllTxn():
            txn_data = get_payload_data(txn)
            if get_type(txn) == NODE and \
                    txn_data.get(DATA, {}).get(SERVICES) is not None:
                srvs.update({txn_data[TARGET_NYM]: txn_data[DATA][SERVICES]})
        return srvs

    @staticmethod
    def updateNodeTxns(oldTxn, newTxn):
        updateNestedDict(oldTxn, newTxn, nestedKeysToUpdate=[DATA, ])
