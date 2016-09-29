"""
A client in an RBFT system.
Client sends requests to each of the nodes,
and receives result of the request execution from nodes.
"""
import base64
import copy
import os
import time
from collections import deque, OrderedDict
from typing import List, Union, Dict, Optional, Mapping, Tuple, Set, Any, \
    Iterable

from ledger.serializers.compact_serializer import CompactSerializer
from plenum.client.pool_manager import HasPoolManager
from plenum.common.exceptions import MissingNodeOp
from plenum.common.has_file_storage import HasFileStorage
from plenum.common.ledger_manager import LedgerManager
from plenum.persistence.client_req_rep_store_file import ClientReqRepStoreFile
from plenum.persistence.client_txn_log import ClientTxnLog
from raet.raeting import AutoMode
from ledger.merkle_verifier import MerkleVerifier
from ledger.util import F, STH

from plenum.client.signer import Signer, SimpleSigner
from plenum.client.wallet import Wallet
from plenum.common.motor import Motor
from plenum.common.plugin_helper import loadPlugins
from plenum.common.raet import getLocalEstateData, getHaFromLocalEstate
from plenum.common.stacked import NodeStack
from plenum.common.startable import Status, LedgerState, Mode
from plenum.common.txn import REPLY, TXN_TYPE, TARGET_NYM, \
    DATA, ALIAS, NEW_NODE, NODE_IP, NODE_PORT, CLIENT_IP, \
    CLIENT_PORT, CHANGE_HA, CHANGE_KEYS, VERKEY, POOL_LEDGER_TXNS, \
    LEDGER_STATUS, CONSISTENCY_PROOF, CATCHUP_REP, USER, STEWARD, NYM, ROLE, \
    REQACK, REQNACK
from plenum.common.types import Request, Reply, OP_FIELD_NAME, f, HA, \
    LedgerStatus, TaggedTuples
from plenum.common.util import getMaxFailures, getlogger, error, hexToCryptonym, \
    MessageProcessor, checkIfMoreThanFSameItems
from plenum.common.txn_util import getTxnOrderedFields
# DEPR
# from plenum.persistence.wallet_storage_file import WalletStorageFile
from plenum.common.util import getConfig

logger = getlogger()


class Client(Motor,
             MessageProcessor,
             HasFileStorage,
             HasPoolManager):
    def __init__(self,
                 name: str,
                 nodeReg: Dict[str, HA]=None,
                 ha: Union[HA, Tuple[str, int]]=None,
                 basedirpath: str=None,
                 config=None):
        """
        Creates a new client.

        :param name: unique identifier for the client
        :param nodeReg: names and host addresses of all nodes in the pool
        :param ha: tuple of host and port
        :param lastReqId: Request Id of the last request sent by client
        """
        self.config = config or getConfig()
        basedirpath = self.config.baseDir if not basedirpath else basedirpath
        self.basedirpath = basedirpath

        cha = None
        # If client information already exists is RAET then use that
        if self.exists(name, basedirpath):
            logger.debug("Client {} ignoring given ha".format(ha))
            cha = getHaFromLocalEstate(name, basedirpath)
            if cha:
                cha = HA(*cha)
            # clientEstate = getLocalEstateData(name, basedirpath)
            # if clientEstate:
            #     cha = HA(*clientEstate["ha"])
        if not cha:
            cha = ha if isinstance(ha, HA) else HA(*ha)


        self.name = name
        self.reqRepStore = self.getReqRepStore()
        self.txnLog = self.getTxnLogStore()
        # DEPR
        # self.lastReqId = lastReqId or self.reqRepStore.lastReqId

        self.dataDir = self.config.clientDataDir or "data/clients"
        HasFileStorage.__init__(self, self.name, baseDir=self.basedirpath,
                                dataDir=self.dataDir)

        self._ledger = None

        if not nodeReg:
            self.mode = None
            HasPoolManager.__init__(self)
            self.ledgerManager = LedgerManager(self, ownedByNode=False)
            self.ledgerManager.addLedger(0, self.ledger,
                 postCatchupCompleteClbk=self.postPoolLedgerCaughtUp,
                 postTxnAddedToLedgerClbk=self.postTxnFromCatchupAddedToLedger)
        else:
            cliNodeReg = OrderedDict()
            for nm, (ip, port) in nodeReg.items():
                cliNodeReg[nm] = HA(ip, port)
            self.nodeReg = cliNodeReg
            self.mode = Mode.discovered

        self.setF()

        stackargs = dict(name=name,
                         ha=cha,
                         main=False,  # stops incoming vacuous joins
                         auto=AutoMode.always)
        stackargs['basedirpath'] = basedirpath
        self.created = time.perf_counter()

        # noinspection PyCallingNonCallable
        self.nodestack = self.nodeStackClass(stackargs,
                                             self.handleOneNodeMsg,
                                             self.nodeReg)
        self.nodestack.onConnsChanged = self.onConnsChanged

        logger.info("Client {} initialized with the following node registry:"
                    .format(name))
        lengths = [max(x) for x in zip(*[
            (len(name), len(host), len(str(port)))
            for name, (host, port) in self.nodeReg.items()])]
        fmt = "    {{:<{}}} listens at {{:<{}}} on port {{:>{}}}".format(
            *lengths)
        for name, (host, port) in self.nodeReg.items():
            logger.info(fmt.format(name, host, port))

        Motor.__init__(self)

        self.inBox = deque()

        self.nodestack.connectNicelyUntil = 0  # don't need to connect
        # nicely as a client

        # TODO: Need to have couple of tests around `reqsPendingConnection`
        # where we check with and without pool ledger
        # Stores the requests that need to be sent to the nodes when the client
        # has made sufficient connections to the nodes.
        self.reqsPendingConnection = deque()

        tp = loadPlugins(self.basedirpath)
        logger.debug("total plugins loaded in client: {}".format(tp))

    def getReqRepStore(self):
        return ClientReqRepStoreFile(self.name, self.basedirpath)

    def getTxnLogStore(self):
        return ClientTxnLog(self.name, self.basedirpath)

    def __repr__(self):
        return self.name

    def postPoolLedgerCaughtUp(self):
        self.mode = Mode.discovered
        # For the scenario where client has already connected to nodes reading
        #  the genesis pool transactions and that is enough
        if self.hasSufficientConnections:
            self.flushMsgsPendingConnection()

    def postTxnFromCatchupAddedToLedger(self, ledgerType: int, txn: Any):
        if ledgerType != 0:
            logger.error("{} got unknown ledger type {}".
                         format(self, ledgerType))
            return
        self.processPoolTxn(txn)

    # noinspection PyAttributeOutsideInit
    def setF(self):
        nodeCount = len(self.nodeReg)
        self.f = getMaxFailures(nodeCount)
        self.minNodesToConnect = self.f + 1
        self.totalNodes = nodeCount

    @staticmethod
    def exists(name, basedirpath):
        return os.path.exists(basedirpath) and \
               os.path.exists(os.path.join(basedirpath, name))

    @property
    def nodeStackClass(self) -> NodeStack:
        return NodeStack

    def start(self, loop):
        oldstatus = self.status
        super().start(loop)
        if oldstatus in Status.going():
            logger.info("{} is already {}, so start has no effect".
                        format(self, self.status.name))
        else:
            self.nodestack.start()
            self.nodestack.maintainConnections()
            if self._ledger:
                self.ledgerManager.setLedgerCanSync(0, True)
                self.mode = Mode.starting

    async def prod(self, limit) -> int:
        """
        async function that returns the number of events

        :param limit: The number of messages to be processed
        :return: The number of events up to a prescribed `limit`
        """
        s = await self.nodestack.service(limit)
        if self.isGoing():
            await self.nodestack.serviceLifecycle()
        self.nodestack.flushOutBoxes()
        # TODO: This if condition has to be removed. `_ledger` if once set wont
        # be reset ever so in `__init__` the `prod` method should be patched.
        if self._ledger:
            s += self.ledgerManager._serviceActions()
        return s

    def createRequest(self, operation: Mapping,
                      identifier: str = None) -> Request:
        """
        Client creates request which include requested operation and request Id

        :param operation: requested operation
        :return: New client request
        """

        request = Request(identifier=identifier or self.defaultIdentifier,
                          operation=operation)
        # DEPR
        # self.setReqId(request)
        return request

    def submitReqs(self, *reqs: Request) -> List[Request]:
        requests = []
        for request in reqs:
            # DEPR
            # self.setReqId(request)
            if self.mode == Mode.discovered and self.hasSufficientConnections:
                self.nodestack.send(request)
            else:
                self.pendReqsTillConnection(request)
            requests.append(request)
        for r in requests:
            self.reqRepStore.addRequest(r)
        return requests

    def handleOneNodeMsg(self, wrappedMsg, excludeFromCli=None) -> None:
        """
        Handles single message from a node, and appends it to a queue
        :param wrappedMsg: Reply received by the client from the node
        """
        self.inBox.append(wrappedMsg)
        msg, frm = wrappedMsg
        # Do not print result of transaction type `POOL_LEDGER_TXNS` on the CLI
        txnTypes = (POOL_LEDGER_TXNS, LEDGER_STATUS, CONSISTENCY_PROOF,
                    CATCHUP_REP)
        printOnCli = not excludeFromCli and msg.get(OP_FIELD_NAME) not \
                                            in txnTypes
        logger.debug("Client {} got msg from node {}: {}".
                     format(self.name, frm, msg),
                     extra={"cli": printOnCli})
        if OP_FIELD_NAME in msg:
            if msg[OP_FIELD_NAME] in txnTypes and self._ledger:
                op = msg.get(OP_FIELD_NAME, None)
                if not op:
                    raise MissingNodeOp
                # TODO: Refactor this copying
                cls = TaggedTuples.get(op, None)
                t = copy.deepcopy(msg)
                t.pop(OP_FIELD_NAME, None)
                cMsg = cls(**t)
                if msg[OP_FIELD_NAME] == POOL_LEDGER_TXNS:
                    self.poolTxnReceived(cMsg, frm)
                if msg[OP_FIELD_NAME] == LEDGER_STATUS:
                    self.ledgerManager.processLedgerStatus(cMsg, frm)
                if msg[OP_FIELD_NAME] == CONSISTENCY_PROOF:
                    self.ledgerManager.processConsistencyProof(cMsg, frm)
                if msg[OP_FIELD_NAME] == CATCHUP_REP:
                    self.ledgerManager.processCatchupRep(cMsg, frm)
            elif msg[OP_FIELD_NAME] == REQACK:
                self.reqRepStore.addAck(msg, frm)
            elif msg[OP_FIELD_NAME] == REQNACK:
                self.reqRepStore.addNack(msg, frm)
            elif msg[OP_FIELD_NAME] == REPLY:
                result = msg[f.RESULT.nm]
                reqId = msg[f.RESULT.nm][f.REQ_ID.nm]
                numReplies = self.reqRepStore.addReply(reqId, frm, result)
                self.postReplyRecvd(reqId, frm, result, numReplies)

    def postReplyRecvd(self, reqId, frm, result, numReplies):
        if not self.txnLog.hasTxnWithReqId(reqId) and numReplies > self.f:
            replies = self.reqRepStore.getReplies(reqId).values()
            reply = checkIfMoreThanFSameItems(replies, self.f)
            if reply:
                self.txnLog.append(reqId, reply)
                return reply

    def _statusChanged(self, old, new):
        # do nothing for now
        pass

    def onStopping(self, *args, **kwargs):
        self.nodestack.nextCheck = 0
        if self.nodestack.opened:
            self.nodestack.close()
        if self._ledger:
            self.ledgerManager.setLedgerState(0, LedgerState.not_synced)
            self.mode = None

    def getReply(self, reqId: int) -> Optional[Reply]:
        """
        Accepts reply message from node if the reply is matching

        :param reqId: Request Id
        :return: Reply message only when valid and matching
        (None, NOT_FOUND)
        (None, UNCONFIRMED) f+1 not reached
        (reply, CONFIRMED) f+1 reached
        """
        try:
            cons = self.hasConsensus(reqId)
        except KeyError:
            return None, "NOT_FOUND"
        if cons:
            return cons, "CONFIRMED"
        return None, "UNCONFIRMED"

    def getRepliesFromAllNodes(self, reqId: int):
        """
        Accepts a request ID and return a list of results from all the nodes
        for that request

        :param reqId: Request ID
        :return: list of request results from all nodes
        """
        return {frm: msg for msg, frm in self.inBox
                if msg[OP_FIELD_NAME] == REPLY and
                msg[f.RESULT.nm][f.REQ_ID.nm] == reqId}

    def hasConsensus(self, reqId: int) -> Optional[str]:
        """
        Accepts a request ID and returns True if consensus was reached
        for the request or else False

        :param reqId: Request ID
        """
        replies = self.getRepliesFromAllNodes(reqId)
        if not replies:
            raise KeyError(reqId)  # NOT_FOUND
        # Check if at least f+1 replies are received or not.
        if self.f + 1 > len(replies):
            return False  # UNCONFIRMED
        else:
            onlyResults = {frm: reply["result"] for frm, reply in
                           replies.items()}
            resultsList = list(onlyResults.values())
            # if all the elements in the resultList are equal - consensus
            # is reached.
            if all(result == resultsList[0] for result in resultsList):
                return resultsList[0]  # CONFIRMED
            else:
                logger.error(
                    "Received a different result from at least one of the nodes..")
                return checkIfMoreThanFSameItems(resultsList, self.f)

    def showReplyDetails(self, reqId: int):
        """
        Accepts a request ID and prints the reply details

        :param reqId: Request ID
        """
        replies = self.getRepliesFromAllNodes(reqId)
        replyInfo = "Node {} replied with result {}"
        if replies:
            for frm, reply in replies.items():
                print(replyInfo.format(frm, reply['result']))
        else:
            print("No replies received from Nodes!")

    def onConnsChanged(self, joined: Set[str], left: Set[str]):
        """
        Modify the current status of the client based on the status of the
        connections changed.
        """
        if self.isGoing():
            if len(self.nodestack.conns) == len(self.nodeReg):
                self.status = Status.started
            elif len(self.nodestack.conns) >= self.minNodesToConnect:
                self.status = Status.started_hungry
            if self.hasSufficientConnections and self.mode == Mode.discovered:
                self.flushMsgsPendingConnection()
        if self._ledger:
            for n in joined:
                self.sendLedgerStatus(n)

    def replyIfConsensus(self, reqId: int):
        replies, errors = self.reqRepStore.getAllReplies(reqId)
        r = list(replies.values())[0] if len(replies) > self.f else None
        e = list(errors.values())[0] if len(errors) > self.f else None
        return r, e

    @property
    def hasSufficientConnections(self):
        return len(self.nodestack.conns) >= self.minNodesToConnect

    def hasMadeRequest(self, reqId: int):
        return self.reqRepStore.hasRequest(reqId)

    def isRequestSuccessful(self, reqId):
        acks = self.reqRepStore.getAcks(reqId)
        nacks = self.reqRepStore.getNacks(reqId)
        f = getMaxFailures(len(self.nodeReg))
        if len(acks) > f:
            return True, "Done"
        elif len(nacks) > f:
            # TODO: What if the the nacks were different from each node?
            return False, list(nacks.values())[0]
        else:
            return None

    def pendReqsTillConnection(self, request, signer=None):
        """
        Enqueue requests that need to be submitted until the client has
        sufficient connections to nodes
        :return:
        """
        self.reqsPendingConnection.append((request, signer))
        logger.debug("Enqueuing request since not enough connections "
                     "with nodes: {}".format(request))

    def flushMsgsPendingConnection(self):
        queueSize = len(self.reqsPendingConnection)
        if queueSize > 0:
            logger.debug("Flushing pending message queue of size {}"
                         .format(queueSize))
            while self.reqsPendingConnection:
                req, signer = self.reqsPendingConnection.popleft()
                self.nodestack.send(req, signer=signer)

    def sendLedgerStatus(self, nodeName: str):
        ledgerStatus = LedgerStatus(0, self.ledger.size, self.ledger.root_hash)
        rid = self.nodestack.getRemote(nodeName).uid
        self.nodestack.send(ledgerStatus, rid)

    def send(self, msg: Any, *rids: Iterable[int], signer: Signer = None):
        self.nodestack.send(msg, *rids, signer=signer)

    @staticmethod
    def verifyMerkleProof(*replies: Tuple[Reply]) -> bool:
        """
        Verifies the correctness of the merkle proof provided in the reply from
        the node. Returns True if verified to be correct, throws an exception
        otherwise.

        :param replies: One or more replies for which Merkle Proofs have to be
        verified
        :raises ProofError: The proof is invalid
        :return: True
        """
        verifier = MerkleVerifier()
        fields = getTxnOrderedFields()
        serializer = CompactSerializer(fields=fields)
        for r in replies:
            seqNo = r[f.RESULT.nm][F.seqNo.name]
            rootHash = base64.b64decode(
                r[f.RESULT.nm][F.rootHash.name].encode())
            auditPath = [base64.b64decode(
                a.encode()) for a in r[f.RESULT.nm][F.auditPath.name]]
            filtered = ((k, v) for (k, v) in r[f.RESULT.nm].iteritems()
                        if k not in
                        [F.auditPath.name, F.seqNo.name, F.rootHash.name])
            result = serializer.serialize(dict(filtered))
            verifier.verify_leaf_inclusion(result, seqNo - 1,
                                           auditPath,
                                           STH(tree_size=seqNo,
                                               sha256_root_hash=rootHash))
        return True


class ClientProvider:
    """
    Lazy client provider that takes a callback that returns a Client.
    It also shadows the client and when the client's any attribute is accessed
    the first time, it creates the client object using the callback.
    """

    def __init__(self, clientGenerator=None):
        """
        :param clientGenerator: Client generator
        """
        self.clientGenerator = clientGenerator
        self.client = None

    def __getattr__(self, attr):
        if attr not in ["clientGenerator", "client"]:
            if not self.client:
                self.client = self.clientGenerator()
            if hasattr(self.client, attr):
                return getattr(self.client, attr)
            raise AttributeError(
                "Client has no attribute named {}".format(attr))
