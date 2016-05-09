import os
import shutil
from binascii import hexlify
from collections import OrderedDict
from typing import Dict, Tuple

from libnacl.encode import base64_decode
from raet.raeting import AutoMode

from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.ledger import Ledger
from plenum.common.raet import initRemoteKeep
from plenum.common.txn import TXN_TYPE, NEW_NODE, TARGET_NYM, DATA, PUBKEY, \
    NODE_IP, ALIAS, NODE_PORT, CLIENT_PORT, NEW_STEWARD, ClientBootStrategy, \
    NEW_CLIENT, TXN_ID, CLIENT, STEWARD
from plenum.common.types import HA
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


class HasPoolManager:
    # noinspection PyUnresolvedReferences
    def __init__(self, nodeRegistry=None, ha=None, cliname=None, cliha=None):
        if not nodeRegistry:
            self.poolManager = TxnPoolManager(self)
            for type in (NEW_NODE, NEW_STEWARD, NEW_CLIENT):
                self.requestExecuter[
                    type] = self.poolManager.executePoolTxnRequest
        else:
            self.poolManager = RegistryPoolManager(self.name, self.basedirpath,
                                                   nodeRegistry, ha, cliname,
                                                   cliha)


class TxnPoolManager(PoolManager):
    def __init__(self, node):
        self.node = node
        self.name = node.name
        self.config = node.config
        self.basedirpath = node.basedirpath
        self.poolTransactionsFile = self.config.poolTransactionsFile
        self.poolTxnStore = self.getPoolTxnStore()
        self.nstack, self.cstack, self.nodeReg = \
            self.getStackParamsAndNodeReg(self.name, self.basedirpath)

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
        ledger = Ledger(CompactMerkleTree(), dataDir=self.node.getDataLocation(),
                        fileName=self.poolTransactionsFile)
        return ledger

    def getStackParamsAndNodeReg(self, name, basedirpath, nodeRegistry=None,
                                 ha=None, cliname=None, cliha=None):
        nstack = None
        cstack = None
        nodeReg = OrderedDict()
        for _, txn in self.poolTxnStore.getAllTxn().items():
            if txn[TXN_TYPE] == NEW_NODE:
                verkey, pubkey = hexlify(
                    base64_decode(txn[TARGET_NYM].encode())), \
                                 txn[DATA][PUBKEY]
                nodeName = txn[DATA][ALIAS]
                nodeHa = (txn[DATA][NODE_IP], txn[DATA][NODE_PORT])
                nodeReg[nodeName] = HA(*nodeHa)
                if nodeName == name:
                    nstack = dict(name=name,
                                  ha=HA('0.0.0.0', txn[DATA][NODE_PORT]),
                                  main=True,
                                  auto=AutoMode.never)
                    cstack = dict(name=nodeName + CLIENT_STACK_SUFFIX,
                                  ha=HA('0.0.0.0', txn[DATA][CLIENT_PORT]),
                                  main=True,
                                  auto=AutoMode.always)
                    if basedirpath:
                        nstack['basedirpath'] = basedirpath
                        cstack['basedirpath'] = basedirpath
                else:
                    try:
                        initRemoteKeep(name, nodeName, basedirpath, pubkey,
                                       verkey)
                    except Exception as ex:
                        print(ex)
            elif txn[TXN_TYPE] in (NEW_STEWARD, NEW_CLIENT) \
                    and self.config.clientBootStrategy == \
                            ClientBootStrategy.PoolTxn:
                self.addNewRole(txn)
        return nstack, cstack, nodeReg

    async def executePoolTxnRequest(self, ppTime, req):
        """
        Execute a transaction that involves consensus pool management, like
        adding a node, client or a steward.

        :param ppTime: PrePrepare request time
        :param req: request
        """
        reply = await self.node.generateReply(ppTime, req)
        op = req.operation
        if op[TXN_TYPE] == NEW_NODE:
            self.addNewNodeAndConnect(op)
        elif op[TXN_TYPE] in (NEW_STEWARD, NEW_CLIENT) and \
                        self.config.clientBootStrategy == \
                        ClientBootStrategy.PoolTxn:
            self.addNewRole(op)
        reply.result.update(op)
        await self.poolTxnStore.append(
            identifier=req.identifier, reply=reply, txnId=reply.result[TXN_ID])
        self.node.transmitToClient(reply,
                                   self.node.clientIdentifiers[req.identifier])

    def addNewRole(self, txn):
        """
        Adds a new client or steward to this node based on transaction type.
        """
        role = STEWARD if txn[TXN_TYPE] == NEW_STEWARD else CLIENT
        identifier = txn[TARGET_NYM]
        verkey = hexlify(base64_decode(txn[TARGET_NYM].encode())).decode()
        pubkey = txn[DATA][PUBKEY]
        self.node.clientAuthNr.addClient(identifier,
                                         verkey=verkey,
                                         pubkey=pubkey,
                                         role=role)

    def addNewNodeAndConnect(self, txn):
        """
        Add a new node to remote keep and connect to it.
        """
        verkey, pubkey = hexlify(base64_decode(txn[TARGET_NYM].encode())), \
                         txn[DATA][PUBKEY]
        nodeName = txn[DATA][ALIAS]
        nodeHa = (txn[DATA][NODE_IP], txn[DATA][NODE_PORT])
        try:
            initRemoteKeep(self.name, nodeName, self.basedirpath, pubkey,
                           verkey)
        except Exception as ex:
            logger.debug("Exception while initializing keep for remote {}".
                         format(ex))
        self.node.nodestack.nodeReg[nodeName] = HA(*nodeHa)


class RegistryPoolManager(PoolManager):
    def __init__(self, name, basedirpath, nodeRegistry, ha, cliname, cliha):
        self.nstack, self.cstack, self.nodeReg = self.getStackParamsAndNodeReg(
            name=name, basedirpath=basedirpath,
            nodeRegistry=nodeRegistry, ha=ha,
            cliname=cliname, cliha=cliha)

    def getStackParamsAndNodeReg(self, name, basedirpath, nodeRegistry=None,
                                 ha=None, cliname=None, cliha=None) -> \
            Tuple[dict, dict, dict]:
        nstack, nodeReg = self.getNodeStackParams(name, nodeRegistry, ha,
                                                  basedirpath=basedirpath)
        cstack = self.getClientStackParams(name, nodeRegistry,
                                           cliname=cliname, cliha=cliha,
                                           basedirpath=basedirpath)
        return nstack, cstack, nodeReg

    @staticmethod
    def getNodeStackParams(name, nodeRegistry: Dict[str, HA],
                           ha: HA = None,
                           basedirpath: str = None) -> Tuple[dict, dict]:
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

        nstack = dict(name=name,
                      ha=ha,
                      main=True,
                      auto=AutoMode.never)

        if basedirpath:
            nstack['basedirpath'] = basedirpath

        return nstack, nodeReg

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
