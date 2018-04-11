"""
A client in an RBFT system.
Client sends requests to each of the nodes,
and receives result of the request execution from nodes.
"""

import copy
import os
import random
import time
import uuid
from collections import deque, OrderedDict
from functools import partial
from typing import List, Union, Dict, Optional, Tuple, Set, Any, \
    Iterable, Callable

from common.serializers.serialization import ledger_txn_serializer, \
    state_roots_serializer, proof_nodes_serializer
from crypto.bls.bls_crypto import BlsCryptoVerifier
from crypto.bls.bls_multi_signature import MultiSignatureValue
from ledger.merkle_verifier import MerkleVerifier
from ledger.util import F, STH
from plenum.bls.bls_crypto_factory import create_default_bls_crypto_factory
from plenum.bls.bls_key_register_pool_ledger import \
    BlsKeyRegisterPoolLedger
from plenum.client.pool_manager import HasPoolManager
from plenum.common.config_util import getConfig
from plenum.common.has_file_storage import HasFileStorage
from plenum.common.ledger import Ledger
from plenum.common.ledger_manager import LedgerManager
from plenum.common.message_processor import MessageProcessor
from plenum.common.messages.node_message_factory import node_message_factory
from plenum.common.messages.node_messages import Reply, LedgerStatus
from plenum.common.motor import Motor
from plenum.common.plugin_helper import loadPlugins
from plenum.common.request import Request
from plenum.common.stacks import nodeStackClass
from plenum.common.startable import Status, Mode
from plenum.common.constants import REPLY, POOL_LEDGER_TXNS, \
    LEDGER_STATUS, CONSISTENCY_PROOF, CATCHUP_REP, REQACK, REQNACK, REJECT, \
    OP_FIELD_NAME, POOL_LEDGER_ID, LedgerState, MULTI_SIGNATURE, MULTI_SIGNATURE_PARTICIPANTS, \
    MULTI_SIGNATURE_SIGNATURE, MULTI_SIGNATURE_VALUE
from plenum.common.txn_util import idr_from_req_data
from plenum.common.types import f
from plenum.common.util import getMaxFailures, rawToFriendly, mostCommonElement
from plenum.persistence.client_req_rep_store_file import ClientReqRepStoreFile
from plenum.persistence.client_txn_log import ClientTxnLog
from plenum.server.has_action_queue import HasActionQueue
from plenum.server.quorums import Quorums
from state.pruning_state import PruningState
from stp_core.common.constants import CONNECTION_PREFIX
from stp_core.common.log import getlogger
from stp_core.crypto.nacl_wrappers import Signer
from stp_core.network.auth_mode import AuthMode
from stp_core.network.exceptions import RemoteNotFound
from stp_core.network.network_interface import NetworkInterface
from stp_core.types import HA
from plenum.common.constants import STATE_PROOF
from plenum.common.tools import lazy_field

logger = getlogger()


class Client(Motor,
             MessageProcessor,
             HasFileStorage,
             HasPoolManager,
             HasActionQueue):
    def __init__(self,
                 name: str,
                 nodeReg: Dict[str, HA] = None,
                 ha: Union[HA, Tuple[str, int]] = None,
                 basedirpath: str = None,
                 genesis_dir: str = None,
                 ledger_dir: str = None,
                 keys_dir: str = None,
                 plugins_dir: str = None,
                 config=None,
                 sighex: str = None):
        """
        Creates a new client.

        :param name: unique identifier for the client
        :param nodeReg: names and host addresses of all nodes in the pool
        :param ha: tuple of host and port
        """
        self.config = config or getConfig()

        dataDir = self.config.clientDataDir or "data/clients"
        self.basedirpath = basedirpath or self.config.CLI_BASE_DIR
        self.basedirpath = os.path.expanduser(self.basedirpath)

        signer = Signer(sighex)
        sighex = signer.keyraw
        verkey = rawToFriendly(signer.verraw)

        self.stackName = verkey
        # TODO: Have a way for a client to have a user friendly name. Does it
        # matter now, it used to matter in some CLI exampples in the past.
        # self.name = name
        self.name = self.stackName or 'Client~' + str(id(self))

        self.genesis_dir = genesis_dir or self.basedirpath
        self.ledger_dir = ledger_dir or os.path.join(self.basedirpath, dataDir, self.name)
        self.plugins_dir = plugins_dir or self.basedirpath
        _keys_dir = keys_dir or self.basedirpath
        self.keys_dir = os.path.join(_keys_dir, "keys")

        cha = None
        if self.exists(self.stackName, self.keys_dir):
            cha = self.nodeStackClass.getHaFromLocal(
                self.stackName, self.keys_dir)
            if cha:
                cha = HA(*cha)
                logger.debug("Client {} ignoring given ha {} and using {}".
                             format(self.name, ha, cha))
        if not cha:
            cha = ha if isinstance(ha, HA) else HA(*ha)

        self.reqRepStore = self.getReqRepStore()
        self.txnLog = self.getTxnLogStore()

        HasFileStorage.__init__(self, self.ledger_dir)

        # TODO: Find a proper name
        self.alias = name

        if not nodeReg:
            self.mode = None
            HasPoolManager.__init__(self)
            self.ledgerManager = LedgerManager(self, ownedByNode=False)
            self.ledgerManager.addLedger(
                POOL_LEDGER_ID,
                self.ledger,
                postCatchupCompleteClbk=self.postPoolLedgerCaughtUp,
                postTxnAddedToLedgerClbk=self.postTxnFromCatchupAddedToLedger)
        else:
            cliNodeReg = OrderedDict()
            for nm, (ip, port) in nodeReg.items():
                cliNodeReg[nm] = HA(ip, port)
            self.nodeReg = cliNodeReg
            self.mode = Mode.discovered

        HasActionQueue.__init__(self)

        self.setPoolParams()

        stackargs = dict(name=self.stackName,
                         ha=cha,
                         main=False,  # stops incoming vacuous joins
                         auth_mode=AuthMode.ALLOW_ANY.value)
        stackargs['basedirpath'] = self.keys_dir
        self.created = time.perf_counter()

        # noinspection PyCallingNonCallable
        # TODO I think this is a bug here, sighex is getting passed in the seed
        # parameter
        self.nodestack = self.nodeStackClass(stackargs,
                                             self.handleOneNodeMsg,
                                             self.nodeReg,
                                             sighex)
        self.nodestack.onConnsChanged = self.onConnsChanged

        if self.nodeReg:
            logger.info(
                "Client {} initialized with the following node registry:".format(
                    self.alias))
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

        self._observers = {}  # type Dict[str, Callable]
        self._observerSet = set()  # makes it easier to guard against duplicates

        plugins_to_load = self.config.PluginsToLoad if hasattr(self.config, "PluginsToLoad") else None
        tp = loadPlugins(self.plugins_dir, plugins_to_load)

        logger.debug("total plugins loaded in client: {}".format(tp))

        self._multi_sig_verifier = self._create_multi_sig_verifier()
        self._read_only_requests = set()

    @lazy_field
    def _bls_register(self):
        return BlsKeyRegisterPoolLedger(self.ledger)

    def _create_multi_sig_verifier(self) -> BlsCryptoVerifier:
        verifier = create_default_bls_crypto_factory() \
            .create_bls_crypto_verifier()
        return verifier

    def getReqRepStore(self):
        return ClientReqRepStoreFile(self.ledger_dir)

    def getTxnLogStore(self):
        return ClientTxnLog(self.ledger_dir)

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
    def setPoolParams(self):
        nodeCount = len(self.nodeReg)
        self.f = getMaxFailures(nodeCount)
        self.minNodesToConnect = self.f + 1
        self.totalNodes = nodeCount
        self.quorums = Quorums(nodeCount)
        logger.info(
            "{} updated its pool parameters: f {}, totalNodes {},"
            "minNodesToConnect {}, quorums {}".format(
                self.alias,
                self.f, self.totalNodes,
                self.minNodesToConnect, self.quorums))

    @staticmethod
    def exists(name, base_dir):
        return os.path.exists(base_dir) and \
            os.path.exists(os.path.join(base_dir, name))

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
            if self.ledger:
                self.ledgerManager.setLedgerCanSync(POOL_LEDGER_ID, True)
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
        if self.ledger:
            s += self.ledgerManager._serviceActions()
        return s

    def submitReqs(self, *reqs: Request) -> Tuple[List[Request], List[str]]:
        requests = []
        errs = []

        for request in reqs:
            is_read_only = request.txn_type in self._read_only_requests
            if self.can_send_request(request):
                recipients = self._connected_node_names
                if is_read_only and len(recipients) > 1:
                    recipients = random.sample(list(recipients), 1)

                logger.debug('Client {} sending request {} to recipients {}'
                             .format(self, request, recipients))

                stat, err_msg = self.sendToNodes(request, names=recipients)

                if stat:
                    self._expect_replies(request, recipients)
                else:
                    errs.append(err_msg)
                    logger.debug(
                        'Client {} request failed {}'.format(self, err_msg))
                    continue
            else:
                logger.debug(
                    "{} pending request since in mode {} and "
                    "connected to {} nodes".format(
                        self, self.mode, self.nodestack.connecteds))
                self.pendReqsTillConnection(request)
            requests.append(request)
        for r in requests:
            self.reqRepStore.addRequest(r)
        return requests, errs

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
            if msg[OP_FIELD_NAME] in ledgerTxnTypes and self.ledger:
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
                self._got_expected(msg, frm)
            elif msg[OP_FIELD_NAME] == REQNACK:
                self.reqRepStore.addNack(msg, frm)
                self._got_expected(msg, frm)
            elif msg[OP_FIELD_NAME] == REJECT:
                self.reqRepStore.addReject(msg, frm)
                self._got_expected(msg, frm)
            elif msg[OP_FIELD_NAME] == REPLY:
                result = msg[f.RESULT.nm]
                identifier = idr_from_req_data(msg[f.RESULT.nm])
                reqId = msg[f.RESULT.nm][f.REQ_ID.nm]
                numReplies = self.reqRepStore.addReply(identifier,
                                                       reqId,
                                                       frm,
                                                       result)

                self._got_expected(msg, frm)
                self.postReplyRecvd(identifier, reqId, frm, result, numReplies)

    def postReplyRecvd(self, identifier, reqId, frm, result, numReplies):
        if not self.txnLog.hasTxn(identifier, reqId):
            reply, _ = self.getReply(identifier, reqId)
            if reply:
                self.txnLog.append(identifier, reqId, reply)
                for name in self._observers:
                    try:
                        self._observers[name](name, reqId, frm, result,
                                              numReplies)
                    except Exception as ex:
                        # TODO: All errors should not be shown on CLI, or maybe we
                        # show errors with different color according to the
                        # severity. Like an error occurring due to node sending
                        # a malformed message should not result in an error message
                        # being shown on the cli since the clients would anyway
                        # collect enough replies from other nodes.
                        logger.debug("Observer threw an exception", exc_info=ex)
                return reply
            # Reply is not verified
            key = (identifier, reqId)
            if key not in self.expectingRepliesFor and numReplies == 1:
                # only one node was asked, but its reply cannot be confirmed,
                # so ask other nodes
                recipients = self._connected_node_names.difference({frm})
                self.resendRequests({
                    (identifier, reqId): recipients
                }, force_expect=True)

    def _statusChanged(self, old, new):
        # do nothing for now
        pass

    def onStopping(self, *args, **kwargs):
        logger.debug('Stopping client {}'.format(self))
        self.nodestack.nextCheck = 0
        self.nodestack.stop()
        if self.ledger:
            self.ledgerManager.setLedgerState(
                POOL_LEDGER_ID, LedgerState.not_synced)
            self.mode = None
            self.ledger.stop()
            if self.hashStore and not self.hashStore.closed:
                self.hashStore.close()
        self.txnLog.close()

    def getReply(self, identifier: str, reqId: int) -> Optional:
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
                idr_from_req_data(msg[f.RESULT.nm]) == identifier}

    def hasConsensus(self, identifier: str, reqId: int) -> Optional[Reply]:
        """
        Accepts a request ID and returns reply for it if quorum achieved or
        there is a state proof for it.

        :param identifier: identifier of the entity making the request
        :param reqId: Request ID
        """
        full_req_id = '({}:{})'.format(identifier, reqId)
        replies = self.getRepliesFromAllNodes(identifier, reqId)
        if not replies:
            raise KeyError(full_req_id)
        proved_reply = self.take_one_proved(replies, full_req_id)
        if proved_reply:
            logger.debug("Found proved reply for {}".format(full_req_id))
            return proved_reply
        quorumed_reply = self.take_one_quorumed(replies, full_req_id)
        if quorumed_reply:
            logger.debug("Reply quorum for {} achieved"
                         .format(full_req_id))
            return quorumed_reply

    def take_one_quorumed(self, replies, full_req_id):
        """
        Checks whether there is sufficint number of equal replies from
        different nodes. It uses following logic:

        1. Check that there are sufficient replies received at all.
           If not - return None.
        2. Check that all these replies are equal.
           If yes - return one of them.
        3. Check that there is a group of equal replies which is large enough.
           If yes - return one reply from this group.
        4. Return None

        """
        if not self.quorums.reply.is_reached(len(replies)):
            return None

        # excluding state proofs from check since they can be different
        def without_state_proof(result):
            if STATE_PROOF in result:
                result.pop(STATE_PROOF)
            return result

        results = [without_state_proof(reply["result"])
                   for reply in replies.values()]

        first = results[0]
        if all(result == first for result in results):
            return first
        logger.debug("Received a different result from "
                     "at least one node for {}"
                     .format(full_req_id))

        result, freq = mostCommonElement(results)
        if not self.quorums.reply.is_reached(freq):
            return None
        return result

    def take_one_proved(self, replies, full_req_id):
        """
        Returns one reply with valid state proof
        """
        for sender, reply in replies.items():
            result = reply['result']
            if STATE_PROOF not in result or result[STATE_PROOF] is None:
                logger.debug("There is no state proof in "
                             "reply for {} from {}"
                             .format(full_req_id, sender))
                continue
            if not self.validate_multi_signature(result[STATE_PROOF]):
                logger.debug("{} got reply for {} with bad "
                             "multi signature from {}"
                             .format(self.name, full_req_id, sender))
                # TODO: do something with this node
                continue
            if not self.validate_proof(result):
                logger.debug("{} got reply for {} with invalid "
                             "state proof from {}"
                             .format(self.name, full_req_id, sender))
                # TODO: do something with this node
                continue
            return result

    def validate_multi_signature(self, state_proof):
        """
        Validates multi signature
        """
        multi_signature = state_proof[MULTI_SIGNATURE]
        if not multi_signature:
            logger.debug("There is a state proof, but no multi signature")
            return False

        participants = multi_signature[MULTI_SIGNATURE_PARTICIPANTS]
        signature = multi_signature[MULTI_SIGNATURE_SIGNATURE]
        value = MultiSignatureValue(
            **(multi_signature[MULTI_SIGNATURE_VALUE])
        ).as_single_value()
        if not self.quorums.bls_signatures.is_reached(len(participants)):
            logger.debug("There is not enough participants of "
                         "multi-signature")
            return False
        public_keys = []
        for node_name in participants:
            key = self._bls_register.get_key_by_name(node_name)
            if key is None:
                logger.debug("There is no bls key for node {}"
                             .format(node_name))
                return False
            public_keys.append(key)
        return self._multi_sig_verifier.verify_multi_sig(signature,
                                                         value,
                                                         public_keys)

    def validate_proof(self, result):
        """
        Validates state proof
        """
        state_root_hash = result[STATE_PROOF]['root_hash']
        state_root_hash = state_roots_serializer.deserialize(state_root_hash)
        proof_nodes = result[STATE_PROOF]['proof_nodes']
        if isinstance(proof_nodes, str):
            proof_nodes = proof_nodes.encode()
        proof_nodes = proof_nodes_serializer.deserialize(proof_nodes)
        key, value = self.prepare_for_state(result)
        valid = PruningState.verify_state_proof(state_root_hash,
                                                key,
                                                value,
                                                proof_nodes,
                                                serialized=True)
        return valid

    def prepare_for_state(self, result) -> tuple:
        # this should be overridden
        pass

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
        if self.ledger:
            for n in joined:
                self.sendLedgerStatus(n)

    @property
    def hasSufficientConnections(self):
        return len(self.nodestack.conns) >= self.minNodesToConnect

    @property
    def hasAnyConnections(self):
        return len(self.nodestack.conns) > 0

    def can_send_write_requests(self):
        if not Mode.is_done_discovering(self.mode):
            return False
        if not self.hasSufficientConnections:
            return False
        return True

    def can_send_read_requests(self):
        if not Mode.is_done_discovering(self.mode):
            return False
        if not self.hasAnyConnections:
            return False
        return True

    def can_send_request(self, request):
        if not Mode.is_done_discovering(self.mode):
            return False
        if self.hasSufficientConnections:
            return True
        if not self.hasAnyConnections:
            return False
        if request.isForced():
            return True
        is_read_only = request.txn_type in self._read_only_requests
        if is_read_only:
            return True
        return False

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
                if self.can_send_request(req):
                    self.send(req, signer=signer)
                else:
                    tmp.append((req, signer))
            self.reqsPendingConnection.extend(tmp)

    def _expect_replies(self, request: Request,
                        nodes: Optional[Set[str]] = None):
        nodes = nodes if nodes else self._connected_node_names
        now = time.perf_counter()
        self.expectingAcksFor[request.key] = (nodes, now, 0)
        self.expectingRepliesFor[request.key] = (copy.copy(nodes), now, 0)
        self.startRepeating(self._retry_for_expected,
                            self.config.CLIENT_REQACK_TIMEOUT)

    @property
    def _connected_node_names(self):
        return {
            remote.name
            for remote in self.nodestack.remotes.values()
            if self.nodestack.isRemoteConnected(remote)
        }

    def _got_expected(self, msg, sender):

        def drop(req, register):
            key = (req.get(f.IDENTIFIER.nm), req.get(f.REQ_ID.nm))
            if key in register:
                received = register[key][0]
                if sender in received:
                    received.remove(sender)
                if not received:
                    register.pop(key)

        if msg[OP_FIELD_NAME] == REQACK:
            drop(msg, self.expectingAcksFor)
        elif msg[OP_FIELD_NAME] == REPLY:
            drop(msg[f.RESULT.nm], self.expectingAcksFor)
            drop(msg[f.RESULT.nm], self.expectingRepliesFor)
        elif msg[OP_FIELD_NAME] in (REQNACK, REJECT):
            drop(msg, self.expectingAcksFor)
            drop(msg, self.expectingRepliesFor)
        else:
            raise RuntimeError("{} cannot retry {}".format(self, msg))

        if not self.expectingAcksFor and not self.expectingRepliesFor:
            self._stop_expecting()

    def _stop_expecting(self):
        self.stopRepeating(self._retry_for_expected, strict=False)

    def _filter_expected(self, now, queue, retry_timeout, max_retry):
        dead_requests = []
        alive_requests = {}
        not_answered_nodes = set()
        for requestKey, (expected_from, last_tried, retries) in queue.items():
            if now < last_tried + retry_timeout:
                continue
            if retries >= max_retry:
                dead_requests.append(requestKey)
                continue
            if requestKey not in alive_requests:
                alive_requests[requestKey] = set()
            alive_requests[requestKey].update(expected_from)
            not_answered_nodes.update(expected_from)
        return dead_requests, alive_requests, not_answered_nodes

    def _retry_for_expected(self):
        now = time.perf_counter()

        requests_with_no_ack, alive_requests, not_acked_nodes = \
            self._filter_expected(now,
                                  self.expectingAcksFor,
                                  self.config.CLIENT_REQACK_TIMEOUT,
                                  self.config.CLIENT_MAX_RETRY_ACK)

        requests_with_no_reply, alive_requests, not_replied_nodes = \
            self._filter_expected(now,
                                  self.expectingRepliesFor,
                                  self.config.CLIENT_REPLY_TIMEOUT,
                                  self.config.CLIENT_MAX_RETRY_REPLY)

        for request_key in requests_with_no_ack:
            logger.debug('{} have got no ACKs for {} and will not try again'
                         .format(self, request_key))
            self.expectingAcksFor.pop(request_key)

        for request_key in requests_with_no_reply:
            logger.debug('{} have got no REPLYs for {} and will not try again'
                         .format(self, request_key))
            self.expectingRepliesFor.pop(request_key)

        if not_acked_nodes:
            logger.debug('{} going to retry for {}'
                         .format(self, self.expectingAcksFor.keys()))

        for node_name in not_acked_nodes:
            try:
                remote = self.nodestack.getRemote(node_name)
            except RemoteNotFound:
                logger.warning('{}{} could not find remote {}'
                               .format(CONNECTION_PREFIX, self, node_name))
                continue
            logger.debug('Remote {} of {} being joined since REQACK for not '
                         'received for request'.format(remote, self))

            # This makes client to reconnect
            # even if pool is just busy and cannot answer quickly,
            # that's why using maintainConnections instead
            # self.nodestack.connect(name=remote.name)
            self.nodestack.maintainConnections(force=True)

        if alive_requests:
            # Need a delay in case connection has to be established with some
            # nodes, a better way is not to assume the delay value but only
            # send requests once the connection is established. Also it is
            # assumed that connection is not established if a node not sending
            # REQACK/REQNACK/REJECT/REPLY, but a little better way is to compare
            # the value in stats of the stack and look for changes in count of
            # `message_reject_rx` but that is not very helpful either since
            # it does not record which node rejected
            delay = 3 if not_acked_nodes else 0
            self._schedule(partial(self.resendRequests, alive_requests), delay)

    def resendRequests(self, keys, force_expect=False):
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
                elif force_expect:
                    queue[key] = (nodes, now, 1)

    def sendLedgerStatus(self, nodeName: str):
        ledgerStatus = LedgerStatus(
            POOL_LEDGER_ID,
            self.ledger.size,
            None,
            None,
            self.ledger.root_hash)
        rid = self.nodestack.getRemote(nodeName).uid
        self.send(ledgerStatus, rid)

    def send(self, msg: Any, *rids: Iterable[int], signer: Signer = None):
        return self.nodestack.send(msg, *rids, signer=signer)

    def sendToNodes(self, msg: Any, names: Iterable[str]):
        rids = [rid for rid, r in self.nodestack.remotes.items()
                if r.name in names]
        return self.send(msg, *rids)

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
        serializer = ledger_txn_serializer
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

    def registerObserver(self, observer: Callable, name=None):
        if not name:
            name = uuid.uuid4()
        if name in self._observers or observer in self._observerSet:
            raise RuntimeError("Observer {} already registered".format(name))
        self._observers[name] = observer
        self._observerSet.add(observer)

    def deregisterObserver(self, name):
        if name not in self._observers:
            raise RuntimeError("Observer {} not registered".format(name))
        self._observerSet.remove(self._observers[name])
        del self._observers[name]

    def hasObserver(self, observer):
        return observer in self._observerSet


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
