from typing import Dict, Tuple

from copy import deepcopy
from ledger.util import F
from raet.raeting import AutoMode

from plenum.common.exceptions import UnsupportedOperation, \
    UnauthorizedClientRequest
from plenum.common.stack_manager import TxnStackManager
from plenum.common.txn import TXN_TYPE, NEW_NODE, TARGET_NYM, DATA, ALIAS, \
    CHANGE_HA, CHANGE_KEYS, POOL_TXN_TYPES
from plenum.common.types import HA, f
from plenum.common.types import NodeDetail, CLIENT_STACK_SUFFIX
from plenum.common.util import getlogger

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
    # noinspection PyUnresolvedReferences, PyTypeChecker
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


class TxnPoolManager(PoolManager, TxnStackManager):
    def __init__(self, node, ha=None, cliname=None, cliha=None):
        self.node = node
        self.name = node.name
        self.config = node.config
        self.basedirpath = node.basedirpath
        self._ledger = None
        TxnStackManager.__init__(self, self.name, self.basedirpath, isNode=True)
        self.nstack, self.cstack, self.nodeReg, self.cliNodeReg = \
            self.getStackParamsAndNodeReg(self.name, self.basedirpath, ha=ha,
                                          cliname=cliname, cliha=cliha)

    @property
    def hasLedger(self):
        return self.node.hasFile(self.ledgerFile)

    @property
    def ledgerLocation(self):
        return self.node.dataLocation

    @property
    def ledgerFile(self):
        return self.config.poolTransactionsFile

    def getStackParamsAndNodeReg(self, name, basedirpath, nodeRegistry=None,
                                 ha=None, cliname=None, cliha=None):
        nodeReg, cliNodeReg, nodeKeys = self.parseLedgerForHaAndKeys()

        self.addRemoteKeysFromLedger(nodeKeys)

        # If node name was not found in the pool transactions file
        if not ha:
            ha = nodeReg[name]

        nstack = dict(name=name,
                      ha=HA('0.0.0.0', ha[1]),
                      main=True,
                      auto=AutoMode.never)
        nodeReg[name] = HA(*ha)

        cliname = cliname or (name + CLIENT_STACK_SUFFIX)
        if not cliha:
            cliha = cliNodeReg[cliname]
        cstack = dict(name=cliname or (name + CLIENT_STACK_SUFFIX),
                      ha=HA('0.0.0.0', cliha[1]),
                      main=True,
                      auto=AutoMode.always)
        cliNodeReg[cliname] = HA(*cliha)

        if basedirpath:
            nstack['basedirpath'] = basedirpath
            cstack['basedirpath'] = basedirpath

        return nstack, cstack, nodeReg, cliNodeReg

    def executePoolTxnRequest(self, ppTime, req):
        """
        Execute a transaction that involves consensus pool management, like
        adding a node, client or a steward.

        :param ppTime: PrePrepare request time
        :param req: request
        """
        reply = self.node.generateReply(ppTime, req)
        op = req.operation
        reply.result.update(op)
        merkleProof = self.node.ledgerManager.appendToLedger(0, reply.result)
        txn = deepcopy(reply.result)
        txn[F.seqNo.name] = merkleProof[F.seqNo.name]
        self.onPoolMembershipChange(txn)
        reply.result.update(merkleProof)
        self.node.transmitToClient(reply,
                                   self.node.clientIdentifiers[req.identifier])

    def onPoolMembershipChange(self, txn):
        if txn[TXN_TYPE] == NEW_NODE:
            self.addNewNodeAndConnect(txn)
            return
        if txn[TXN_TYPE] == CHANGE_HA:
            self.nodeHaChanged(txn)
            return
        if txn[TXN_TYPE] == CHANGE_KEYS:
            self.nodeKeysChanged(txn)
            return

    def addNewNodeAndConnect(self, txn):
        nodeName = txn[DATA][ALIAS]
        if nodeName == self.name:
            logger.debug("{} not adding itself to node registry".
                         format(self.name))
            return
        self.addNewRemoteAndConnect(txn, nodeName, self.node)
        self.node.newNodeJoined(txn)

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
            rid = self.stackHaChanged(txn, nodeName, self.node)
            self.node.nodestack.outBoxes.pop(rid, None)
            self.node.sendPoolInfoToClients(txn)

    def nodeKeysChanged(self, txn):
        nodeNym = txn[TARGET_NYM]
        nodeName = self.getNodeName(nodeNym)
        if nodeName == self.name:
            logger.debug("{} not changing itself's keep".
                         format(self.name))
            return
        else:
            self.stackKeysChanged(txn, nodeName, self.node)
            self.node.sendPoolInfoToClients(txn)

    def getNodeName(self, nym):
        for txn in self.ledger.getAllTxn().values():
            if txn[TXN_TYPE] == NEW_NODE and txn[TARGET_NYM] == nym:
                return txn[DATA][ALIAS]
        raise Exception("Node with nym {} not found".format(nym))

    def checkRequestAuthorized(self, request):
        typ = request.operation.get(TXN_TYPE)
        error = None
        if typ == NEW_NODE:
            error = self.authErrorWhileAddingNode(request)
        if typ in (CHANGE_HA, CHANGE_KEYS):
            error = self.authErrorWhileUpdatingNode(request)
        if error:
            raise UnauthorizedClientRequest(request.identifier, request.reqId,
                                            error)

    def authErrorWhileAddingNode(self, request):
        origin = request.identifier
        isSteward = self.node.secondaryStorage.isSteward(origin)
        if not isSteward:
            return "{} is not a steward so cannot add a new node".format(origin)
        for txn in self.ledger.getAllTxn().values():
            if txn[TXN_TYPE] == NEW_NODE:
                if txn[f.IDENTIFIER.nm] == origin:
                    return "{} already has a node with name {}".\
                        format(origin, txn[DATA][ALIAS])
                if txn[DATA] == request.operation[DATA]:
                    return "transaction data {} has conflicts with " \
                           "request data {}". \
                        format(txn[DATA], request.operation[DATA])

    def authErrorWhileUpdatingNode(self, request):
        origin = request.identifier
        isSteward = self.node.secondaryStorage.isSteward(origin)
        if not isSteward:
            return "{} is not a steward so cannot add a new node".format(origin)
        for txn in self.ledger.getAllTxn().values():
            if txn[TXN_TYPE] == NEW_NODE and txn[TARGET_NYM] == \
                    request.operation[TARGET_NYM] and txn[f.IDENTIFIER.nm] == origin:
                return
        return "{} is not a steward of node {}".\
            format(origin, request.data[TARGET_NYM])

    @property
    def merkleRootHash(self):
        return self.ledger.root_hash

    @property
    def txnSeqNo(self):
        return self.ledger.seqNo


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
