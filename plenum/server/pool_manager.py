import ipaddress

from abc import abstractmethod
from collections import OrderedDict

from typing import Optional

from copy import deepcopy
from typing import List

from common.exceptions import LogicError
from common.serializers.serialization import state_roots_serializer
from storage.helper import initKeyValueStorage
from state.pruning_state import PruningState
from stp_core.common.log import getlogger
from stp_core.network.auth_mode import AuthMode
from stp_core.network.exceptions import RemoteNotFound
from stp_core.types import HA

from plenum.common.constants import NODE, TARGET_NYM, DATA, ALIAS, \
    NODE_IP, NODE_PORT, CLIENT_IP, CLIENT_PORT, VERKEY, SERVICES, \
    VALIDATOR, CLIENT_STACK_SUFFIX, POOL_LEDGER_ID, DOMAIN_LEDGER_ID, BLS_KEY
from plenum.common.stack_manager import TxnStackManager
from plenum.common.txn_util import get_type, get_payload_data
from plenum.persistence.util import pop_merkle_info
from plenum.server.pool_req_handler import PoolRequestHandler

logger = getlogger()


class PoolManager:
    @abstractmethod
    def getStackParamsAndNodeReg(self, name, keys_dir, nodeRegistry=None,
                                 ha=None, cliname=None, cliha=None):
        """
        Returns a tuple(nodestack, clientstack, nodeReg)
        """

    @property
    @abstractmethod
    def merkleRootHash(self) -> str:
        """
        """

    @property
    @abstractmethod
    def txnSeqNo(self) -> int:
        """
        """

    @staticmethod
    def _get_rank(needle_id: str, haystack_ids: List[str]):
        # Return the rank of the node where rank is defined by the order in
        # which node was added to the pool
        try:
            return haystack_ids.index(needle_id)
        except ValueError:
            return None

    @property
    @abstractmethod
    def id(self):
        """
        """

    @abstractmethod
    def get_rank_of(self, node_id, nodeReg=None) -> Optional[int]:
        """Return node rank among active pool validators by id

        :param node_id: node's id
        :param nodeReg: (optional) node registry to operate with. If not specified,
                        current one is used.
        :return: rank of the node or None if not found
        """

    @property
    def rank(self) -> Optional[int]:
        # Nodes have a total order defined in them, rank is the node's
        # position in that order
        return self.get_rank_of(self.id)

    @abstractmethod
    def get_name_by_rank(self, rank, nodeReg=None) -> Optional[str]:
        # Needed for communicating primary name to others and also nodeReg
        # uses node names (alias) and not ids
        # TODO: Should move to using node ids and not node names (alias)
        """Return node name (alias) by rank among active pool validators

        :param rank: rank of the node
        :param nodeReg: (optional) node registry to operate with. If not specified,
                        current one is used.
        :return: name of the node or None if not found
        """


class HasPoolManager:
    # noinspection PyUnresolvedReferences, PyTypeChecker
    def __init__(self, ha=None, cliname=None, cliha=None):
        self.poolManager = TxnPoolManager(self, ha=ha, cliname=cliname,
                                          cliha=cliha)
        self.register_executer(POOL_LEDGER_ID, self.poolManager.executePoolTxnBatch)


class TxnPoolManager(PoolManager, TxnStackManager):
    def __init__(self, node, ha=None, cliname=None, cliha=None):
        self.node = node
        self.name = node.name
        self.config = node.config
        self.genesis_dir = node.genesis_dir
        self.keys_dir = node.keys_dir
        self._ledger = None
        self._id = None

        TxnStackManager.__init__(
            self, self.name, node.genesis_dir, node.keys_dir, isNode=True)
        self.state = self.loadState()
        self.reqHandler = self.getPoolReqHandler()
        self.initPoolState()
        self._load_nodes_order_from_ledger()
        self.nstack, self.cstack, self.nodeReg, self.cliNodeReg = \
            self.getStackParamsAndNodeReg(self.name, self.keys_dir, ha=ha,
                                          cliname=cliname, cliha=cliha)

        self._dataFieldsValidators = (
            (NODE_IP, self._isIpAddressValid),
            (CLIENT_IP, self._isIpAddressValid),
            (NODE_PORT, self._isPortValid),
            (CLIENT_PORT, self._isPortValid),
        )

    def __repr__(self):
        return self.node.name

    def getPoolReqHandler(self):
        return PoolRequestHandler(self.ledger, self.state,
                                  self.node.states[DOMAIN_LEDGER_ID])

    def loadState(self):
        return PruningState(
            initKeyValueStorage(
                self.config.poolStateStorage,
                self.node.dataLocation,
                self.config.poolStateDbName,
                db_config=self.config.db_state_config)
        )

    def initPoolState(self):
        self.node.initStateFromLedger(self.state, self.ledger, self.reqHandler)
        logger.info(
            "{} initialized pool state: state root {}".format(self, state_roots_serializer.serialize(
                bytes(self.state.committedHeadHash))))

    @property
    def hasLedger(self):
        return self.node.hasFile(self.ledgerFile)

    @property
    def ledgerLocation(self):
        return self.node.dataLocation

    @property
    def ledgerFile(self):
        return self.config.poolTransactionsFile

    def getStackParamsAndNodeReg(self, name, keys_dir, nodeRegistry=None,
                                 ha=None, cliname=None, cliha=None):
        nodeReg, cliNodeReg, nodeKeys = self.parseLedgerForHaAndKeys(
            self.ledger)

        self.addRemoteKeysFromLedger(nodeKeys)

        # If node name was not found in the pool transactions file
        if not ha:
            ha = nodeReg[name]

        nstack = dict(name=name,
                      ha=HA(*ha),
                      main=True,
                      auth_mode=AuthMode.RESTRICTED.value,
                      queue_size=self.config.ZMQ_NODE_QUEUE_SIZE)

        cliname = cliname or (name + CLIENT_STACK_SUFFIX)
        if not cliha:
            cliha = cliNodeReg[cliname]
        cstack = dict(name=cliname or (name + CLIENT_STACK_SUFFIX),
                      ha=HA(*cliha),
                      main=True,
                      auth_mode=AuthMode.ALLOW_ANY.value,
                      queue_size=self.config.ZMQ_CLIENT_QUEUE_SIZE)

        if keys_dir:
            nstack['basedirpath'] = keys_dir
            cstack['basedirpath'] = keys_dir

        return nstack, cstack, nodeReg, cliNodeReg

    def executePoolTxnBatch(self, ppTime, reqs, stateRoot, txnRoot) -> List:
        """
        Execute a transaction that involves consensus pool management, like
        adding a node, client or a steward.

        :param ppTime: PrePrepare request time
        :param reqs: request
        """
        committedTxns = self.reqHandler.commit(len(reqs), stateRoot, txnRoot, ppTime)
        self.node.updateSeqNoMap(committedTxns, POOL_LEDGER_ID)
        for txn in committedTxns:
            t = deepcopy(txn)
            # Since the committed transactions contain merkle info,
            # try to avoid this kind of strictness
            pop_merkle_info(t)
            self.onPoolMembershipChange(t)
        self.node.sendRepliesToClients(committedTxns, ppTime)
        return committedTxns

    def onPoolMembershipChange(self, txn):
        if get_type(txn) != NODE:
            return

        txn_data = get_payload_data(txn)
        if DATA not in txn_data:
            return

        nodeName = txn_data[DATA][ALIAS]
        nodeNym = txn_data[TARGET_NYM]

        self._set_node_order(nodeNym, nodeName)

        def _updateNode(txn_data):
            if SERVICES in txn_data[DATA]:
                self.nodeServicesChanged(txn_data)
            if txn_data[DATA][ALIAS] in self.node.nodeReg:
                if {NODE_IP, NODE_PORT, CLIENT_IP, CLIENT_PORT}. \
                        intersection(set(txn_data[DATA].keys())):
                    self.nodeHaChanged(txn_data)
                if VERKEY in txn_data:
                    self.nodeKeysChanged(txn_data)
                if BLS_KEY in txn_data[DATA]:
                    self.node_blskey_changed(txn_data)

        seqNos, info = self.getNodeInfoFromLedger(nodeNym)

        # `onPoolMembershipChange` method can be called only after txn added to ledger
        if len(seqNos) == 0:
            raise LogicError("There are no txns in ledger for nym {}".format(nodeNym))

        # If there is only one transaction has been made to this nym,
        # that means, that this is a new node transaction
        if len(seqNos) == 1:
            if VALIDATOR in txn_data[DATA].get(SERVICES, []):
                self.addNewNodeAndConnect(txn_data)
        else:
            _updateNode(txn_data)

    def addNewNodeAndConnect(self, txn_data):
        nodeName = txn_data[DATA][ALIAS]
        if nodeName == self.name:
            logger.debug("{} adding itself to node registry".
                         format(self.name))
            self.node.nodeReg[nodeName] = HA(txn_data[DATA][NODE_IP],
                                             txn_data[DATA][NODE_PORT])
            self.node.cliNodeReg[nodeName + CLIENT_STACK_SUFFIX] = \
                HA(txn_data[DATA][CLIENT_IP],
                   txn_data[DATA][CLIENT_PORT])
        else:
            self.connectNewRemote(txn_data, nodeName, self.node, nodeName != self.name)
        self.node.nodeJoined(txn_data)

    def node_about_to_be_disconnected(self, nodeName):
        if self.node.master_primary_name == nodeName:
            self.node.view_changer.on_primary_about_to_be_disconnected()

    def nodeHaChanged(self, txn_data):
        nodeNym = txn_data[TARGET_NYM]
        nodeName = self.getNodeName(nodeNym)
        # TODO: Check if new HA is same as old HA and only update if
        # new HA is different.
        if nodeName == self.name:
            # Update itself in node registry if needed
            ha_changed = False
            (ip, port) = self.node.nodeReg[nodeName]
            if NODE_IP in txn_data[DATA] and ip != txn_data[DATA][NODE_IP]:
                ip = txn_data[DATA][NODE_IP]
                ha_changed = True

            if NODE_PORT in txn_data[DATA] and port != txn_data[DATA][NODE_PORT]:
                port = txn_data[DATA][NODE_PORT]
                ha_changed = True

            if ha_changed:
                self.node.nodeReg[nodeName] = HA(ip, port)

            ha_changed = False
            (ip, port) = self.node.cliNodeReg[nodeName + CLIENT_STACK_SUFFIX]
            if CLIENT_IP in txn_data[DATA] and ip != txn_data[DATA][CLIENT_IP]:
                ip = txn_data[DATA][CLIENT_IP]
                ha_changed = True

            if CLIENT_PORT in txn_data[DATA] and port != txn_data[DATA][CLIENT_PORT]:
                port = txn_data[DATA][CLIENT_PORT]
                ha_changed = True

            if ha_changed:
                self.node.cliNodeReg[nodeName + CLIENT_STACK_SUFFIX] = HA(ip, port)

            self.node.nodestack.onHostAddressChanged()
            self.node.clientstack.onHostAddressChanged()
        else:
            rid = self.stackHaChanged(txn_data, nodeName, self.node)
            if rid:
                self.node.nodestack.outBoxes.pop(rid, None)
        self.node_about_to_be_disconnected(nodeName)

    def nodeKeysChanged(self, txn_data):
        # TODO: if the node whose keys are being changed is primary for any
        # protocol instance, then we should trigger an election for that
        # protocol instance. For doing that, for every replica of that
        # protocol instance, `_primaryName` as None, and then the node should
        # call its `decidePrimaries`.
        nodeNym = txn_data[TARGET_NYM]
        nodeName = self.getNodeName(nodeNym)
        # TODO: Check if new keys are same as old keys and only update if
        # new keys are different.
        if nodeName == self.name:
            # TODO: Why?
            logger.debug("{} not changing itself's keep".
                         format(self.name))
            return
        else:
            rid = self.stackKeysChanged(txn_data, nodeName, self.node)
            if rid:
                self.node.nodestack.outBoxes.pop(rid, None)
        self.node_about_to_be_disconnected(nodeName)

    def nodeServicesChanged(self, txn_data):
        nodeNym = txn_data[TARGET_NYM]
        _, nodeInfo = self.getNodeInfoFromLedger(nodeNym)
        nodeName = nodeInfo[DATA][ALIAS]
        oldServices = set(nodeInfo[DATA].get(SERVICES, []))
        newServices = set(txn_data[DATA].get(SERVICES, []))
        if oldServices == newServices:
            logger.info("Node {} not changing {} since it is same as existing".format(nodeNym, SERVICES))
            return
        else:
            if VALIDATOR in newServices.difference(oldServices):
                # If validator service is enabled
                self.node.nodeReg[nodeName] = HA(nodeInfo[DATA][NODE_IP],
                                                 nodeInfo[DATA][NODE_PORT])
                self.node.cliNodeReg[nodeName + CLIENT_STACK_SUFFIX] = HA(
                    nodeInfo[DATA][CLIENT_IP], nodeInfo[DATA][CLIENT_PORT])
                self.updateNodeTxns(nodeInfo, txn_data)
                self.node.nodeJoined(txn_data)

                if self.name != nodeName:
                    self.connectNewRemote(nodeInfo, nodeName, self.node)

            if VALIDATOR in oldServices.difference(newServices):
                # If validator service is disabled
                del self.node.nodeReg[nodeName]
                del self.node.cliNodeReg[nodeName + CLIENT_STACK_SUFFIX]
                self.node.nodeLeft(txn_data)

                if self.name != nodeName:
                    try:
                        rid = TxnStackManager.removeRemote(
                            self.node.nodestack, nodeName)
                        if rid:
                            self.node.nodestack.outBoxes.pop(rid, None)
                    except RemoteNotFound:
                        logger.info('{} did not find remote {} to remove'.format(self, nodeName))
                    self.node_about_to_be_disconnected(nodeName)

    def node_blskey_changed(self, txn_data):
        # if BLS key changes for my Node, then re-init BLS crypto signer with new keys
        node_nym = txn_data[TARGET_NYM]
        node_name = self.getNodeName(node_nym)
        if node_name == self.name:
            bls_key = txn_data[DATA][BLS_KEY]
            self.node.update_bls_key(bls_key)

    def getNodeName(self, nym):
        # Assuming ALIAS does not change
        _, nodeTxn = self.getNodeInfoFromLedger(nym)
        return nodeTxn[DATA][ALIAS]

    @property
    def merkleRootHash(self) -> str:
        return self.ledger.root_hash

    @property
    def txnSeqNo(self) -> int:
        return self.ledger.seqNo

    def getNodeData(self, nym):
        _, nodeTxn = self.getNodeInfoFromLedger(nym)
        return nodeTxn[DATA]

    # Question: Why are `_isIpAddressValid` and `_isPortValid` part of
    # pool_manager?
    @staticmethod
    def _isIpAddressValid(ipAddress):
        try:
            ipaddress.ip_address(ipAddress)
        except ValueError:
            return False
        else:
            return ipAddress != '0.0.0.0'

    @staticmethod
    def _isPortValid(port):
        return isinstance(port, int) and 0 < port <= 65535

    @property
    def id(self):
        if not self._id:
            for _, txn in self.ledger.getAllTxn():
                txn_data = get_payload_data(txn)
                if self.name == txn_data[DATA][ALIAS]:
                    self._id = txn_data[TARGET_NYM]
        return self._id

    def _load_nodes_order_from_ledger(self):
        self._ordered_node_ids = OrderedDict()
        for _, txn in self.ledger.getAllTxn():
            if get_type(txn) == NODE:
                txn_data = get_payload_data(txn)
                self._set_node_order(txn_data[TARGET_NYM], txn_data[DATA][ALIAS])

    def _set_node_order(self, nodeNym, nodeName):
        curName = self._ordered_node_ids.get(nodeNym)
        if curName is None:
            self._ordered_node_ids[nodeNym] = nodeName
            logger.info("{} sets node {} ({}) order to {}".format(
                self.name, nodeName, nodeNym,
                len(self._ordered_node_ids[nodeNym])))
        elif curName != nodeName:
            msg = "{} is trying to order already ordered node {} ({}) with other alias {}" \
                .format(self.name, curName, nodeNym, nodeName)
            logger.error(msg)
            raise LogicError(msg)

    def node_ids_ordered_by_rank(self, nodeReg=None) -> List:
        if nodeReg is None:
            nodeReg = self.nodeReg
        return [nym for nym, name in self._ordered_node_ids.items()
                if name in nodeReg]

    def get_rank_of(self, node_id, nodeReg=None) -> Optional[int]:
        if self.id is None:
            # This can happen if a non-genesis node starts
            return None
        return self._get_rank(node_id, self.node_ids_ordered_by_rank(nodeReg))

    def get_rank_by_name(self, name, nodeReg=None) -> Optional[int]:
        for nym, nm in self._ordered_node_ids.items():
            if name == nm:
                return self.get_rank_of(nym, nodeReg)

    def get_name_by_rank(self, rank, nodeReg=None) -> Optional[str]:
        try:
            nym = self.node_ids_ordered_by_rank(nodeReg)[rank]
        except IndexError:
            return None
        else:
            return self._ordered_node_ids[nym]

    def get_nym_by_name(self, node_name) -> Optional[str]:
        for nym, name in self._ordered_node_ids.items():
            if name == node_name:
                return nym
        return None
