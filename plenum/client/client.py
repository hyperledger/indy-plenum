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
from functools import partial
from typing import List, Union, Dict, Optional, Tuple, Set, Any, \
    Iterable

from plenum.common.ledger import Ledger
from plenum.common.messages.node_message_factory import node_message_factory
from plenum.common.stacks import nodeStackClass
from plenum.server.quorums import Quorums
from stp_core.crypto.nacl_wrappers import Signer
from stp_core.network.auth_mode import AuthMode
from stp_core.network.network_interface import NetworkInterface
from stp_core.types import HA

from ledger.merkle_verifier import MerkleVerifier
from ledger.serializers.compact_serializer import CompactSerializer
from ledger.util import F, STH
from plenum.client.pool_manager import HasPoolManager
from plenum.common.config_util import getConfig
from plenum.common.exceptions import MissingNodeOp
from stp_core.network.exceptions import RemoteNotFound
from plenum.common.has_file_storage import HasFileStorage
from plenum.common.ledger_manager import LedgerManager
from stp_core.common.log import getlogger
from plenum.common.motor import Motor
from plenum.common.plugin_helper import loadPlugins
from plenum.common.request import Request
from plenum.common.startable import Status, Mode
from plenum.common.constants import REPLY, POOL_LEDGER_TXNS, \
    LEDGER_STATUS, CONSISTENCY_PROOF, CATCHUP_REP, REQACK, REQNACK, REJECT, OP_FIELD_NAME, \
    POOL_LEDGER_ID, TXN_TIME, LedgerState
from plenum.common.txn_util import getTxnOrderedFields
from plenum.common.types import f
from plenum.common.messages.node_messages import Reply, LedgerStatus
from plenum.common.util import getMaxFailures, checkIfMoreThanFSameItems, rawToFriendly
from plenum.common.message_processor import MessageProcessor
from plenum.persistence.client_req_rep_store_file import ClientReqRepStoreFile
from plenum.persistence.client_txn_log import ClientTxnLog
from plenum.server.has_action_queue import HasActionQueue

logger = getlogger()


class Client(Motor,
             MessageProcessor,
             HasFileStorage,
             HasPoolManager,
             HasActionQueue):
    def __init__(self,
                 name: str,
                 nodeReg: Dict[str, HA]=None,
                 ha: Union[HA, Tuple[str, int]]=None,
                 basedirpath: str=None,
                 config=None,
                 sighex: str=None):
        """
        Creates a new client.

        :param name: unique identifier for the client
        :param nodeReg: names and host addresses of all nodes in the pool
        :param ha: tuple of host and port
        """
        self.config = config or getConfig()
        basedirpath = self.config.baseDir if not basedirpath else basedirpath
        self.basedirpath = basedirpath

        signer = Signer(sighex)
        sighex = signer.keyraw
        verkey = rawToFriendly(signer.verraw)

        self.stackName = verkey
        # TODO: Have a way for a client to have a user friendly name. Does it
        # matter now, it used to matter in some CLI exampples in the past.
        # self.name = name
        self.name = self.stackName or 'Client~' + str(id(self))

        cha = None
        # If client information already exists is RAET then use that
        if self.exists(self.stackName, basedirpath):
            cha = self.nodeStackClass.getHaFromLocal(self.stackName, basedirpath)
            if cha:
                cha = HA(*cha)
                logger.debug("Client {} ignoring given ha {} and using {}".
                             format(self.name, ha, cha))
        if not cha:
            cha = ha if isinstance(ha, HA) else HA(*ha)

        self.reqRepStore = self.getReqRepStore()
        self.txnLog = self.getTxnLogStore()

        self.dataDir = self.config.clientDataDir or "data/clients"
        HasFileStorage.__init__(self, self.name, baseDir=self.basedirpath,
                                dataDir=self.dataDir)

        # TODO: Find a proper name
        self.alias = name

        self._ledger = None

        if not nodeReg:
            self.mode = None
            HasPoolManager.__init__(self)
            self.ledgerManager = LedgerManager(self, ownedByNode=False)
            self.ledgerManager.addLedger(POOL_LEDGER_ID, self.ledger,
                postCatchupCompleteClbk=self.postPoolLedgerCaughtUp,
                postTxnAddedToLedgerClbk=self.postTxnFromCatchupAddedToLedger)
        else:
            cliNodeReg = OrderedDict()
            for nm, (ip, port) in nodeReg.items():
                cliNodeReg[nm] = HA(ip, port)
            self.nodeReg = cliNodeReg
            self.mode = Mode.discovered

        HasActionQueue.__init__(self)

        self.setF()

        stackargs = dict(name=self.stackName,
                         ha=cha,
                         main=False,  # stops incoming vacuous joins
                         auth_mode=AuthMode.ALLOW_ANY.value)
        stackargs['basedirpath'] = basedirpath
        self.created = time.perf_counter()

        # noinspection PyCallingNonCallable
        # TODO I think this is a bug here, sighex is getting passed in the seed parameter
        self.nodestack = self.nodeStackClass(stackargs,
                                             self.handleOneNodeMsg,
                                             self.nodeReg,
                                             sighex)
        self.nodestack.onConnsChanged = self.onConnsChanged

        if self.nodeReg:
            logger.info("Client {} initialized with the following node registry:"
                        .format(self.alias))
            lengths = [max(x) for x in zip(*[
                (len(name), len(host), len(str(port)))
                for name, (host, port) in self.nodeReg.items()])]
            fmt = "    {{:<{}}} listens at {{:<{}}} on port {{:>{}}}".format(
                *lengths)
            for name, (host, port) in self.nodeReg.items():
                logger.info(fmt.format(name, host, port))
        else:
            logger.info(
                "Client {} found an empty node registry:".format(self.alias))

        Motor.__init__(self)

        self.inBox = deque()

        self.nodestack.connectNicelyUntil = 0  # don't need to connect
        # nicely as a client

        # TODO: Need to have couple of tests around `reqsPendingConnection`
        # where we check with and without pool ledger

        # Stores the requests that need to be sent to the nodes when the client
        # has made sufficient connections to the nodes.
        self.reqsPendingConnection = deque()

        # Tuple of identifier and reqId as key and value as tuple of set of
        # nodes which are expected to send REQACK
        self.expectingAcksFor = {}

        # Tuple of identifier and reqId as key and value as tuple of set of
        # nodes which are expected to send REPLY
        self.expectingRepliesFor = {}

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
        self.quorums = Quorums(nodeCount)

    @staticmethod
    def exists(name, basedirpath):
        return os.path.exists(basedirpath) and \
               os.path.exists(os.path.join(basedirpath, name))

    @property
    def nodeStackClass(self) -> NetworkInterface:
        return nodeStackClass

    def start(self, loop):
        oldstatus = self.status
        if oldstatus in Status.going():
            logger.info("{} is already {}, so start has no effect".
                        format(self.alias, self.status.name))
        else:
            super().start(loop)
            self.nodestack.start()
            self.nodestack.maintainConnections(force=True)
            if self._ledger:
                self.ledgerManager.setLedgerCanSync(0, True)
                self.mode = Mode.starting

    async def prod(self, limit) -> int:
        """
        async function that returns the number of events

        :param limit: The number of messages to be processed
        :return: The number of events up to a prescribed `limit`
        """
        s = 0
        if self.isGoing():
            s = await self.nodestack.service(limit)
            self.nodestack.serviceLifecycle()
        self.nodestack.flushOutBoxes()
        s += self._serviceActions()
        # TODO: This if condition has to be removed. `_ledger` if once set wont
        # be reset ever so in `__init__` the `prod` method should be patched.
        if self._ledger:
            s += self.ledgerManager._serviceActions()
        return s

    def submitReqs(self, *reqs: Request) -> List[Request]:
        requests = []
        for request in reqs:
            if (self.mode == Mode.discovered and self.hasSufficientConnections) or \
               (request.isForced() and self.hasAnyConnections):
                logger.debug('Client {} sending request {}'.format(self, request))
                self.send(request)
                self.expectingFor(request)
            else:
                logger.debug("{} pending request since in mode {} and "
                             "connected to {} nodes".
                             format(self, self.mode, self.nodestack.connecteds))
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
        ledgerTxnTypes = (POOL_LEDGER_TXNS, LEDGER_STATUS, CONSISTENCY_PROOF,
                          CATCHUP_REP)
        printOnCli = not excludeFromCli and msg.get(OP_FIELD_NAME) not \
                                            in ledgerTxnTypes
        logger.info("Client {} got msg from node {}: {}".
                     format(self.name, frm, msg),
                     extra={"cli": printOnCli})
        if OP_FIELD_NAME in msg:
            if msg[OP_FIELD_NAME] in ledgerTxnTypes and self._ledger:
                cMsg = node_message_factory.get_instance(**msg)
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
                self.gotExpected(msg, frm)
            elif msg[OP_FIELD_NAME] == REQNACK:
                self.reqRepStore.addNack(msg, frm)
                self.gotExpected(msg, frm)
            elif msg[OP_FIELD_NAME] == REJECT:
                self.reqRepStore.addReject(msg, frm)
                self.gotExpected(msg, frm)
            elif msg[OP_FIELD_NAME] == REPLY:
                result = msg[f.RESULT.nm]
                identifier = msg[f.RESULT.nm][f.IDENTIFIER.nm]
                reqId = msg[f.RESULT.nm][f.REQ_ID.nm]
                numReplies = self.reqRepStore.addReply(identifier, reqId, frm,
                                                       result)
                self.gotExpected(msg, frm)
                self.postReplyRecvd(identifier, reqId, frm, result, numReplies)

    def postReplyRecvd(self, identifier, reqId, frm, result, numReplies):
        if not self.txnLog.hasTxn(identifier, reqId) and numReplies > self.f:
            replies = self.reqRepStore.getReplies(identifier, reqId).values()
            reply = checkIfMoreThanFSameItems(replies, self.f)
            if reply:
                self.txnLog.append(identifier, reqId, reply)
                return reply

    def _statusChanged(self, old, new):
        # do nothing for now
        pass

    def onStopping(self, *args, **kwargs):
        logger.debug('Stopping client {}'.format(self))
        self.nodestack.nextCheck = 0
        self.nodestack.stop()
        if self._ledger:
            self.ledgerManager.setLedgerState(POOL_LEDGER_ID, LedgerState.not_synced)
            self.mode = None
            self._ledger.stop()
            if self.hashStore and not self.hashStore.closed:
                self.hashStore.close()
        self.txnLog.close()

    def getReply(self, identifier: str, reqId: int) -> Optional[Reply]:
        """
        Accepts reply message from node if the reply is matching

        :param identifier: identifier of the entity making the request
        :param reqId: Request Id
        :return: Reply message only when valid and matching
        (None, NOT_FOUND)
        (None, UNCONFIRMED) f+1 not reached
        (reply, CONFIRMED) f+1 reached
        """
        try:
            cons = self.hasConsensus(identifier, reqId)
        except KeyError:
            return None, "NOT_FOUND"
        if cons:
            return cons, "CONFIRMED"
        return None, "UNCONFIRMED"

    def getRepliesFromAllNodes(self, identifier: str, reqId: int):
        """
        Accepts a request ID and return a list of results from all the nodes
        for that request

        :param identifier: identifier of the entity making the request
        :param reqId: Request ID
        :return: list of request results from all nodes
        """
        return {frm: msg for msg, frm in self.inBox
                if msg[OP_FIELD_NAME] == REPLY and
                msg[f.RESULT.nm][f.REQ_ID.nm] == reqId and
                msg[f.RESULT.nm][f.IDENTIFIER.nm] == identifier}

    def hasConsensus(self, identifier: str, reqId: int) -> Optional[str]:
        """
        Accepts a request ID and returns True if consensus was reached
        for the request or else False

        :param identifier: identifier of the entity making the request
        :param reqId: Request ID
        """
        replies = self.getRepliesFromAllNodes(identifier, reqId)
        if not replies:
            raise KeyError('{}{}'.format(identifier, reqId))  # NOT_FOUND
        # Check if at least f+1 replies are received or not.
        if self.quorums.reply.is_reached(len(replies)):
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
        else:
            return False  # UNCONFIRMED

    def showReplyDetails(self, identifier: str, reqId: int):
        """
        Accepts a request ID and prints the reply details

        :param identifier: Client's identifier
        :param reqId: Request ID
        """
        replies = self.getRepliesFromAllNodes(identifier, reqId)
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
            self.flushMsgsPendingConnection()
        if self._ledger:
            for n in joined:
                self.sendLedgerStatus(n)

    def replyIfConsensus(self, identifier, reqId: int):
        replies, errors = self.reqRepStore.getAllReplies(identifier, reqId)
        r = list(replies.values())[0] if len(replies) > self.f else None
        e = list(errors.values())[0] if len(errors) > self.f else None
        return r, e

    @property
    def hasSufficientConnections(self):
        return len(self.nodestack.conns) >= self.minNodesToConnect

    @property
    def hasAnyConnections(self):
        return len(self.nodestack.conns) > 0

    def hasMadeRequest(self, identifier, reqId: int):
        return self.reqRepStore.hasRequest(identifier, reqId)

    def isRequestSuccessful(self, identifier, reqId):
        acks = self.reqRepStore.getAcks(identifier, reqId)
        nacks = self.reqRepStore.getNacks(identifier, reqId)
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
        logger.debug("{} enqueuing request since not enough connections "
                     "with nodes: {}".format(self, request))

    def flushMsgsPendingConnection(self):
        queueSize = len(self.reqsPendingConnection)
        if queueSize > 0:
            logger.debug("Flushing pending message queue of size {}"
                         .format(queueSize))
            tmp = deque()
            while self.reqsPendingConnection:
                req, signer = self.reqsPendingConnection.popleft()
                if (self.hasSufficientConnections and self.mode == Mode.discovered) or \
                   (req.isForced() and self.hasAnyConnections):
                    self.send(req, signer=signer)
                else:
                    tmp.append((req, signer))
            self.reqsPendingConnection.extend(tmp)

    def expectingFor(self, request: Request, nodes: Optional[Set[str]]=None):
        nodes = nodes or {r.name for r in self.nodestack.remotes.values()
                          if self.nodestack.isRemoteConnected(r)}
        now = time.perf_counter()
        self.expectingAcksFor[request.key] = (nodes, now, 0)
        self.expectingRepliesFor[request.key] = (copy.copy(nodes), now, 0)
        self.startRepeating(self.retryForExpected,
                            self.config.CLIENT_REQACK_TIMEOUT)

    def gotExpected(self, msg, frm):
        if msg[OP_FIELD_NAME] == REQACK:
            container = msg
            colls = (self.expectingAcksFor, )
        elif msg[OP_FIELD_NAME] == REPLY:
            container = msg[f.RESULT.nm]
            # If an REQACK sent by node was lost, the request when sent again
            # would fetch the reply or the client might just lose REQACK and not
            # REPLY so when REPLY received, request does not need to be resent
            colls = (self.expectingAcksFor, self.expectingRepliesFor)
        elif msg[OP_FIELD_NAME] in (REQNACK, REJECT):
            container = msg
            colls = (self.expectingAcksFor, self.expectingRepliesFor)
        else:
            raise RuntimeError("{} cannot retry {}".format(self, msg))

        idr = container.get(f.IDENTIFIER.nm)
        reqId = container.get(f.REQ_ID.nm)
        key = (idr, reqId)
        for coll in colls:
            if key in coll:
                if frm in coll[key][0]:
                    coll[key][0].remove(frm)
                if not coll[key][0]:
                    coll.pop(key)

        if not (self.expectingAcksFor or self.expectingRepliesFor):
            self.stopRetrying()

    def stopRetrying(self):
        self.stopRepeating(self.retryForExpected, strict=False)

    def _filterExpected(self, now, queue, retryTimeout, maxRetry):
        deadRequests = []
        aliveRequests = {}
        notAnsweredNodes = set()
        for requestKey, (expectedFrom, lastTried, retries) in queue.items():
            if now < lastTried + retryTimeout:
                continue
            if retries >= maxRetry:
                deadRequests.append(requestKey)
                continue
            if requestKey not in aliveRequests:
                aliveRequests[requestKey] = set()
            aliveRequests[requestKey].update(expectedFrom)
            notAnsweredNodes.update(expectedFrom)
        return deadRequests, aliveRequests, notAnsweredNodes

    def retryForExpected(self):
        now = time.perf_counter()

        requestsWithNoAck, aliveRequests, notAckedNodes = \
            self._filterExpected(now,
                                 self.expectingAcksFor,
                                 self.config.CLIENT_REQACK_TIMEOUT,
                                 self.config.CLIENT_MAX_RETRY_ACK)

        requestsWithNoReply, aliveRequests, notRepliedNodes = \
            self._filterExpected(now,
                                 self.expectingRepliesFor,
                                 self.config.CLIENT_REPLY_TIMEOUT,
                                 self.config.CLIENT_MAX_RETRY_REPLY)

        for requestKey in requestsWithNoAck:
            logger.debug('{} have got no ACKs for {} and will not try again'
                         .format(self, requestKey))
            self.expectingAcksFor.pop(requestKey)

        for requestKey in requestsWithNoReply:
            logger.debug('{} have got no REPLYs for {} and will not try again'
                         .format(self, requestKey))
            self.expectingRepliesFor.pop(requestKey)

        if notAckedNodes:
            logger.debug('{} going to retry for {}'
                         .format(self, self.expectingAcksFor.keys()))
        for nm in notAckedNodes:
            try:
                remote = self.nodestack.getRemote(nm)
            except RemoteNotFound:
                logger.warning('{} could not find remote {}'.format(self, nm))
                continue
            logger.debug('Remote {} of {} being joined since REQACK for not '
                         'received for request'.format(remote, self))


            # This makes client to reconnect
            # even if pool is just busy and cannot answer quickly,
            # that's why using maintainConnections instead
            # self.nodestack.connect(name=remote.name)
            self.nodestack.maintainConnections(force=True)

        if aliveRequests:
            # Need a delay in case connection has to be established with some
            # nodes, a better way is not to assume the delay value but only
            # send requests once the connection is established. Also it is
            # assumed that connection is not established if a node not sending
            # REQACK/REQNACK/REJECT/REPLY, but a little better way is to compare
            # the value in stats of the stack and look for changes in count of
            # `message_reject_rx` but that is not very helpful either since
            # it does not record which node rejected
            delay = 3 if notAckedNodes else 0
            self._schedule(partial(self.resendRequests, aliveRequests), delay)

    def resendRequests(self, keys):
        for key, nodes in keys.items():
            if not nodes:
                continue
            request = self.reqRepStore.getRequest(*key)
            logger.debug('{} resending request {} to {}'.
                         format(self, request, nodes))
            self.sendToNodes(request, nodes)
            now = time.perf_counter()
            for queue in [self.expectingAcksFor, self.expectingRepliesFor]:
                if key in queue:
                    _, _, retries = queue[key]
                    queue[key] = (nodes, now, retries + 1)

    def sendLedgerStatus(self, nodeName: str):
        ledgerStatus = LedgerStatus(POOL_LEDGER_ID, self.ledger.size, None, None,
                                    self.ledger.root_hash)
        rid = self.nodestack.getRemote(nodeName).uid
        self.send(ledgerStatus, rid)

    def send(self, msg: Any, *rids: Iterable[int], signer: Signer = None):
        self.nodestack.send(msg, *rids, signer=signer)

    def sendToNodes(self, msg: Any, names: Iterable[str]):
        rids = [rid for rid, r in self.nodestack.remotes.items() if r.name in names]
        self.send(msg, *rids)

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
        ignored = {F.auditPath.name, F.seqNo.name, F.rootHash.name}
        for r in replies:
            seqNo = r[f.RESULT.nm][F.seqNo.name]
            rootHash = Ledger.strToHash(
                r[f.RESULT.nm][F.rootHash.name])
            auditPath = [Ledger.strToHash(a) for a in
                         r[f.RESULT.nm][F.auditPath.name]]
            filtered = dict((k, v) for (k, v) in r[f.RESULT.nm].items()
                            if k not in ignored)
            result = serializer.serialize(filtered)
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
