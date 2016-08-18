import os
import shutil
from binascii import hexlify
from collections import OrderedDict
from typing import Dict, Tuple

from libnacl.encode import base64_decode
from raet.raeting import AutoMode

from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.ledger import Ledger
from ledger.stores.file_hash_store import FileHashStore
from plenum.common.exceptions import UnsupportedOperation, \
    UnauthorizedClientRequest

from plenum.common.raet import initRemoteKeep
from plenum.common.types import HA, f
from plenum.common.txn import TXN_TYPE, NEW_NODE, TARGET_NYM, DATA, PUBKEY, \
    NODE_IP, ALIAS, NODE_PORT, CLIENT_PORT, NEW_STEWARD, ClientBootStrategy, \
    NEW_CLIENT, TXN_ID, CLIENT, STEWARD, CLIENT_IP, CHANGE_HA, CHANGE_KEYS, \
    POOL_TXN_TYPES, VERKEY
from plenum.common.util import getlogger
from plenum.common.types import NodeDetail, CLIENT_STACK_SUFFIX


logger = getlogger()


class PoolManager:
    def getStackParamsAndNodeReg(self, name, basedirpath, nodeRegistry=None,
                                 ha=None, cliname=None, cliha=None):
        """
        Returns a tuple(nodestack, clientstack, nodeReg)
        """
        raise NotImplementedError

    @property
    def merkleRootHash(self):
        raise NotImplementedError

    @property
    def txnSeqNo(self):
        raise NotImplementedError


class HasPoolManager:
    # noinspection PyUnresolvedReferences
    def __init__(self, nodeRegistry=None, ha=None, cliname=None, cliha=None):
        if not nodeRegistry:
            self.poolManager = TxnPoolManager(self, ha=ha, cliname=cliname,
                                              cliha=cliha)
            for types in POOL_TXN_TYPES:
                self.requestExecuter[types] = \
                    self.poolManager.executePoolTxnRequest
        else:
            self.poolManager = RegistryPoolManager(self.name, self.basedirpath,
                                                   nodeRegistry, ha, cliname,
                                                   cliha)


class TxnPoolManager(PoolManager):
    def __init__(self, node, ha=None, cliname=None, cliha=None):
        self.node = node
        self.name = node.name
        self.config = node.config
        self.basedirpath = node.basedirpath
        self.poolTransactionsFile = self.config.poolTransactionsFile
        self.poolTxnStore = self.getPoolTxnStore()
        self.nstack, self.cstack, self.nodeReg, self.cliNodeReg = \
            self.getStackParamsAndNodeReg(self.name, self.basedirpath, ha=ha,
                                          cliname=cliname, cliha=cliha)

    def getPoolTxnStore(self) -> Ledger:
        """
        Create and return the pool transaction ledger.
        """
        basedirpath = self.basedirpath
        if not self.node.hasFile(self.poolTransactionsFile):
            defaultTxnFile = os.path.join(basedirpath,
                                          self.poolTransactionsFile)
            if not os.path.isfile(defaultTxnFile):
                raise FileNotFoundError("Pool transactions file not found")
            else:
                shutil.copy(defaultTxnFile, self.node.getDataLocation())

        dataDir = self.node.getDataLocation()
        ledger = Ledger(CompactMerkleTree(hashStore=FileHashStore(
            dataDir=dataDir)),
            dataDir=dataDir,
            fileName=self.poolTransactionsFile)
        return ledger

    def getStackParamsAndNodeReg(self, name, basedirpath, nodeRegistry=None,
                                 ha=None, cliname=None, cliha=None):
        nstack = None
        cstack = None
        nodeReg = OrderedDict()
        cliNodeReg = OrderedDict()
        nodeKeys = {}
        for _, txn in self.poolTxnStore.getAllTxn().items():
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

                if nodeName == name:
                    if nHa and cHa:
                        nstack = dict(name=name,
                                      ha=HA('0.0.0.0', nHa[1]),
                                      main=True,
                                      auto=AutoMode.never)
                        cstack = dict(name=name + CLIENT_STACK_SUFFIX,
                                      ha=HA('0.0.0.0', cHa[1]),
                                      main=True,
                                      auto=AutoMode.always)
                else:
                    if txn[TXN_TYPE] in (NEW_NODE, CHANGE_KEYS):
                        verkey = hexlify(base64_decode(txn[TARGET_NYM].encode()))
                        nodeKeys[nodeName] = verkey
            elif txn[TXN_TYPE] in (NEW_STEWARD, NEW_CLIENT) \
                    and self.config.clientBootStrategy == \
                            ClientBootStrategy.PoolTxn:
                self.addNewRole(txn)

        for nm, keys in nodeKeys.items():
            try:
                initRemoteKeep(name, nm, basedirpath, nodeKeys[nm],
                               override=True)
            except Exception as ex:
                print(ex)

        # If node name was not found in the pool transactions file
        if nstack is None or ha is not None:
            nstack = dict(name=name,
                          ha=HA('0.0.0.0', ha[1]),
                          main=True,
                          auto=AutoMode.never)
            nodeReg[name] = HA(*ha)

        if cstack is None or cliha is not None:
            cstack = dict(name=cliname or (name + CLIENT_STACK_SUFFIX),
                          ha=HA('0.0.0.0', cliha[1]),
                          main=True,
                          auto=AutoMode.always)
        if basedirpath:
            nstack['basedirpath'] = basedirpath
            cstack['basedirpath'] = basedirpath

        return nstack, cstack, nodeReg, cliNodeReg

    async def executePoolTxnRequest(self, ppTime, req):
        """
        Execute a transaction that involves consensus pool management, like
        adding a node, client or a steward.

        :param ppTime: PrePrepare request time
        :param req: request
        """
        reply = await self.node.generateReply(ppTime, req)
        op = req.operation
        self.onPoolMembershipChange(op)
        reply.result.update(op)
        merkleProof = await self.poolTxnStore.append(
            identifier=req.identifier, reply=reply, txnId=reply.result[TXN_ID])
        reply.result.update(merkleProof)
        self.node.transmitToClient(reply,
                                   self.node.clientIdentifiers[req.identifier])

    def onPoolMembershipChange(self, op):
        if op[TXN_TYPE] == NEW_NODE:
            self.addNewNodeAndConnect(op)
            return
        if op[TXN_TYPE] in (NEW_STEWARD, NEW_CLIENT) and \
                        self.config.clientBootStrategy == \
                        ClientBootStrategy.PoolTxn:
            self.addNewRole(op)
            return
        if op[TXN_TYPE] == CHANGE_HA:
            self.nodeHaChanged(op)
            return
        if op[TXN_TYPE] == CHANGE_KEYS:
            self.nodeKeysChanged(op)
            return

    def addNewNodeAndConnect(self, txn):
        nodeName = txn[DATA][ALIAS]
        if nodeName == self.name:
            logger.debug("{} not adding itself to node registry".
                         format(self.name))
            return
        verkey = hexlify(base64_decode(txn[TARGET_NYM].encode()))
        try:
            # Override any keys found, reason being the scenario where
            # before this node comes to know about the other node, the other
            # node tries to connect to it.
            initRemoteKeep(self.name, nodeName, self.basedirpath, verkey,
                           override=True)
        except Exception as ex:
            logger.error("Exception while initializing keep for remote {}".
                         format(ex))

        nodeHa = (txn[DATA][NODE_IP], txn[DATA][NODE_PORT])
        cliHa = (txn[DATA][CLIENT_IP], txn[DATA][CLIENT_PORT])
        self.node.nodeReg[nodeName] = HA(*nodeHa)
        self.node.cliNodeReg[nodeName+CLIENT_STACK_SUFFIX] = HA(*cliHa)
        logger.debug("{} adding new node {} with HA {}".format(self.name,
                                                               nodeName, nodeHa))
        self.node.newNodeJoined(nodeName)

    def addNewRole(self, txn):
        """
        Adds a new client or steward to this node based on transaction type.
        """
        identifier = txn[TARGET_NYM]
        if identifier not in self.node.clientAuthNr.clients:
            if txn[TXN_TYPE] == NEW_STEWARD:
                role = STEWARD
            elif txn[TXN_TYPE] == NEW_CLIENT:
                role = CLIENT
            else:
                logger.error("Unable to get role from transaction type {}"
                             .format(txn[TXN_TYPE]))
                return
            verkey = hexlify(base64_decode(txn[TARGET_NYM].encode())).decode()
            self.node.clientAuthNr.addClient(identifier,
                                             verkey=verkey,
                                             role=role)

    def nodeHaChanged(self, txn):
        nodeNym = txn[TARGET_NYM]
        nodeName = self.getNodeName(nodeNym)
        if nodeName == self.name:
            logger.debug("{} clearing local data in keep".
                         format(self.node.nodestack.name))
            self.node.nodestack.keep.clearLocalData()
            logger.debug("{} clearing local data in keep".
                         format(self.node.clientstack.name))
            self.node.clientstack.keep.clearLocalData()
        else:
            logger.debug("{} removing remote {}".format(self.node, nodeName))
            rid = self.node.nodestack.removeRemoteByName(nodeName)
            self.node.nodestack.outBoxes.pop(rid, None)
            nodeHa = (txn[DATA][NODE_IP], txn[DATA][NODE_PORT])
            cliHa = (txn[DATA][CLIENT_IP], txn[DATA][CLIENT_PORT])
            self.node.nodeReg[nodeName] = HA(*nodeHa)
            self.node.cliNodeReg[nodeName + CLIENT_STACK_SUFFIX] = HA(*cliHa)
            self.node.sendNodeHaToClients(nodeName)

    def nodeKeysChanged(self, txn):
        nodeNym = txn[TARGET_NYM]
        nodeName = self.getNodeName(nodeNym)
        if nodeName == self.name:
            logger.debug("{} not changing itself's keep".
                         format(self.name))
            return
        else:
            logger.debug("{} clearing remote role data in keep of {}".
                         format(self.node.nodestack.name, nodeName))
            self.node.nodestack.keep.clearRemoteRoleData(nodeName)
            verkey = txn[DATA][VERKEY]
            try:
                # Override any keys found
                initRemoteKeep(self.name, nodeName, self.basedirpath, verkey,
                               override=True)
            except Exception as ex:
                logger.error("Exception while initializing keep for remote {}".
                             format(ex))
            logger.debug(
                "{} removing remote {}".format(self.node, nodeName))
            # Removing remote so that the nodestack will attempt to connect
            self.node.nodestack.removeRemoteByName(nodeName)

    def getNodeName(self, nym):
        for txn in self.poolTxnStore.getAllTxn().values():
            if txn[TXN_TYPE] == NEW_NODE and txn[TARGET_NYM] == nym:
                return txn[DATA][ALIAS]
        raise Exception("Node with nym {} not found".format(nym))

    def checkRequestAuthorized(self, request):
        typ = request.operation.get(TXN_TYPE)
        error = None
        if typ == NEW_STEWARD:
            error = self.authErrorWhileAddingSteward(request)
        if typ == NEW_NODE:
            error = self.authErrorWhileAddingNode(request)
        if typ in (CHANGE_HA, CHANGE_KEYS):
            error = self.authErrorWhileUpdatingNode(request)
        if error:
            raise UnauthorizedClientRequest(request.identifier, request.reqId,
                                            error)

    def authErrorWhileAddingSteward(self, request):
        origin = request.identifier
        for txn in self.poolTxnStore.getAllTxn().values():
            if txn[TXN_TYPE] == NEW_STEWARD and txn[TARGET_NYM] == origin:
                break
        else:
            return "{} is not a steward so cannot add a new steward". \
                format(origin)
        if self.stewardThresholdExceeded():
            return "New stewards cannot be added by other stewards as "\
                "there are already {} stewards in the system".format(
                    self.config.stewardThreshold)

    def authErrorWhileAddingNode(self, request):
        origin = request.identifier
        isSteward = False
        for txn in self.poolTxnStore.getAllTxn().values():
            if txn[TXN_TYPE] == NEW_STEWARD and txn[TARGET_NYM] == origin:
                isSteward = True
            if txn[TXN_TYPE] == NEW_NODE:
                if txn[f.IDENTIFIER.nm] == origin:
                    return "{} already has a node with name {}".\
                        format(origin, txn[DATA][ALIAS])
                if txn[DATA] == request.operation[DATA]:
                    return "transaction data {} has conflicts with " \
                           "request data {}". \
                        format(txn[DATA], request.operation[DATA])
        if not isSteward:
            return "{} is not a steward so cannot add a new node".format(origin)

    def authErrorWhileUpdatingNode(self, request):
        origin = request.identifier
        isSteward = False
        for txn in self.poolTxnStore.getAllTxn().values():
            if txn[TXN_TYPE] == NEW_STEWARD and txn[TARGET_NYM] == origin:
                isSteward = True
            if txn[TXN_TYPE] == NEW_NODE and txn[TARGET_NYM] == \
                    request.operation[TARGET_NYM] and txn[f.IDENTIFIER.nm] == origin:
                return
        if not isSteward:
            return "{} is not a steward so cannot add a new node".format(origin)
        return "{} is not a steward of node {}".\
            format(origin, request.data[TARGET_NYM])

    def stewardThresholdExceeded(self) -> bool:
        """We allow at most `stewardThreshold` number of  stewards to be added
        by other stewards"""
        return self.countStewards() > self.config.stewardThreshold

    def countStewards(self) -> int:
        """Count the number of stewards added to the pool transaction store"""
        allTxns = self.poolTxnStore.getAllTxn().values()
        stewards = filter(lambda txn: txn[TXN_TYPE] == NEW_STEWARD, allTxns)
        return sum(1 for _ in stewards)

    @property
    def merkleRootHash(self):
        return self.poolTxnStore.root_hash

    @property
    def txnSeqNo(self):
        return self.poolTxnStore.seqNo


class RegistryPoolManager(PoolManager):
    def __init__(self, name, basedirpath, nodeRegistry, ha, cliname, cliha):

        self.nstack, self.cstack, self.nodeReg, self.cliNodeReg = \
            self.getStackParamsAndNodeReg(name=name, basedirpath=basedirpath,
                                          nodeRegistry=nodeRegistry, ha=ha,
                                          cliname=cliname, cliha=cliha)

    def getStackParamsAndNodeReg(self, name, basedirpath, nodeRegistry=None,
                                 ha=None, cliname=None, cliha=None):
        nstack, nodeReg, cliNodeReg = self.getNodeStackParams(name,
                                                              nodeRegistry,
                                                              ha,
                                                              basedirpath=basedirpath)

        cstack = self.getClientStackParams(name, nodeRegistry,
                                           cliname=cliname, cliha=cliha,
                                           basedirpath=basedirpath)

        return nstack, cstack, nodeReg, cliNodeReg

    @staticmethod
    def getNodeStackParams(name, nodeRegistry: Dict[str, HA],
                           ha: HA = None,
                           basedirpath: str = None) -> Tuple[dict, dict, dict]:
        """
        Return tuple(nodeStack params, nodeReg)
        """
        me = nodeRegistry[name]
        if isinstance(me, NodeDetail):
            sha = me.ha
            nodeReg = {k: v.ha for k, v in nodeRegistry.items()}
        else:
            sha = me if isinstance(me, HA) else HA(*me[0])
            nodeReg = {k: v if isinstance(v, HA) else HA(*v[0])
                       for k, v in nodeRegistry.items()}
        if not ha:  # pull it from the registry
            ha = sha

        cliNodeReg = {r.cliname: r.cliha for r in nodeRegistry.values()}

        nstack = dict(name=name,
                      ha=ha,
                      main=True,
                      auto=AutoMode.never)

        if basedirpath:
            nstack['basedirpath'] = basedirpath

        return nstack, nodeReg, cliNodeReg

    @staticmethod
    def getClientStackParams(name, nodeRegistry: Dict[str, HA], cliname,
                             cliha, basedirpath) -> dict:
        """
        Return clientStack params
        """
        me = nodeRegistry[name]
        if isinstance(me, NodeDetail):
            sha = me.ha
            scliname = me.cliname
            scliha = me.cliha
        else:
            sha = me if isinstance(me, HA) else HA(*me[0])
            scliname = None
            scliha = None

        if not cliname:  # default to the name plus the suffix
            cliname = scliname if scliname else name + CLIENT_STACK_SUFFIX
        if not cliha:  # default to same ip, port + 1
            cliha = scliha if scliha else HA(sha[0], sha[1] + 1)

        cstack = dict(name=cliname,
                      ha=cliha,
                      main=True,
                      auto=AutoMode.always)

        if basedirpath:
            cstack['basedirpath'] = basedirpath

        return cstack

    @property
    def merkleRootHash(self):
        raise UnsupportedOperation

    @property
    def txnSeqNo(self):
        raise UnsupportedOperation
