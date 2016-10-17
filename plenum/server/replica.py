import time
from collections import deque, OrderedDict
from enum import IntEnum
from enum import unique
from typing import Dict
from typing import Optional, Any
from typing import Set
from typing import Tuple

import plenum.server.node
from plenum.common.exceptions import SuspiciousNode
from plenum.common.types import ReqDigest, PrePrepare, \
    Prepare, Commit, Ordered, ThreePhaseMsg, ThreePhaseKey
from plenum.common.util import MessageProcessor
from plenum.common.log import getlogger
from plenum.server.models import Commits, Prepares
from plenum.server.router import Router
from plenum.server.suspicion_codes import Suspicions

logger = getlogger()


@unique
class TPCStat(IntEnum):  # TPC => Three-Phase Commit
    ReqDigestRcvd = 0
    PrePrepareSent = 1
    PrePrepareRcvd = 2
    PrepareRcvd = 3
    PrepareSent = 4
    CommitRcvd = 5
    CommitSent = 6
    OrderSent = 7


class Stats:
    def __init__(self, keys):
        sort = sorted([k.value for k in keys])
        self.stats = OrderedDict((s, 0) for s in sort)

    def inc(self, key):
        """
        Increment the stat specified by key.
        """
        self.stats[key] += 1

    def __repr__(self):
        return OrderedDict((TPCStat(k).name, v)
                           for k, v in self.stats.items())


class Replica(MessageProcessor):
    def __init__(self, node: 'plenum.server.node.Node', instId: int,
                 isMaster: bool = False):
        """
        Create a new replica.

        :param node: Node on which this replica is located
        :param instId: the id of the protocol instance the replica belongs to
        :param isMaster: is this a replica of the master protocol instance
        """
        super().__init__()
        self.stats = Stats(TPCStat)

        routerArgs = [(ReqDigest, self._preProcessReqDigest)]

        for r in [PrePrepare, Prepare, Commit]:
            routerArgs.append((r, self.processThreePhaseMsg))
        self.inBoxRouter = Router(*routerArgs)

        self.threePhaseRouter = Router(
                (PrePrepare, self.processPrePrepare),
                (Prepare, self.processPrepare),
                (Commit, self.processCommit)
        )

        self.node = node
        self.instId = instId

        self.name = self.generateName(node.name, self.instId)

        self.outBox = deque()
        """
        This queue is used by the replica to send messages to its node. Replica
        puts messages that are consumed by its node
        """

        self.inBox = deque()
        """
        This queue is used by the replica to receive messages from its node.
        Node puts messages that are consumed by the replica
        """

        self.inBoxStash = deque()
        """
        If messages need to go back on the queue, they go here temporarily and
        are put back on the queue on a state change
        """

        self.isMaster = isMaster

        # Indicates name of the primary replica of this protocol instance.
        # None in case the replica does not know who the primary of the
        # instance is
        self._primaryName = None    # type: Optional[str]

        # Requests waiting to be processed once the replica is able to decide
        # whether it is primary or not
        self.postElectionMsgs = deque()

        # Requests that are stored by non primary replica for which it is
        # expecting corresponding pre prepare requests Dictionary that stores
        # a tuple of client id and request id(sequence no) as key and digest as
        # value. Not creating a set of Tuple3(identifier, reqId, digest) as such a
        # big hashable element is not good. Also this way we can look for the
        # request on the basis of (identifier, reqId) and compare the digest with
        # the received PrePrepare request's digest.
        self.reqsPendingPrePrepare = {}
        # type: Dict[Tuple[str, int], str]

        # PREPARE that are stored by non primary replica for which it has not
        #  got any PRE-PREPARE. Dictionary that stores a tuple of view no and
        #  prepare sequence number as key and a deque of PREPAREs as value.
        # This deque is attempted to be flushed on receiving every
        # PRE-PREPARE request.
        self.preparesWaitingForPrePrepare = {}
        # type: Dict[Tuple[int, int], deque]

        # Dictionary of sent PRE-PREPARE that are stored by primary replica
        # which it has broadcasted to all other non primary replicas
        # Key of dictionary is a 2 element tuple with elements viewNo,
        # pre-prepare seqNo and value is a Request Digest
        self.sentPrePrepares = {}
        # type: Dict[Tuple[int, int], Tuple[ReqDigest, float]]

        # Dictionary of received PRE-PREPAREs. Key of dictionary is a 2
        # element tuple with elements viewNo, pre-prepare seqNo and value is
        # a Request Digest
        self.prePrepares = {}
        # type: Dict[Tuple[int, int], Tuple[ReqDigest, float]]

        self.prePrepareSeqNo = 0  # type: int

        # Dictionary of received Prepare requests. Key of dictionary is a 2
        # element tuple with elements viewNo, seqNo and value is a 2 element
        # tuple containing request digest and set of sender node names(sender
        # replica names in case of multiple protocol instances)
        # (viewNo, seqNo) -> (digest, {senders})
        self.prepares = Prepares()
        # type: Dict[Tuple[int, int], Tuple[str, Set[str]]]

        self.commits = Commits()

        # Set of tuples to keep track of ordered requests
        self.ordered = set()        # type: Set[Tuple[int, int]]

        # Requests with sufficient commits so they can be ordered but have not
        # received request digest from node and neither PRE-PREPARE or PREPARE.
        # Key can be a digest to a tuple of viewNo and ppSeqNo
        self.commitsPendedForOrdering = {}

        # Dictionary to keep track of the which replica was primary during each
        # view. Key is the view no and value is the name of the primary
        # replica during that view
        self.primaryNames = {}  # type: Dict[int, str]

        # Holds msgs that are for later views
        self.threePhaseMsgsForLaterView = deque()
        # type: deque[(ThreePhaseMsg, str)]

        # Holds tuple of view no and prepare seq no of 3-phase messages it
        # received while it was not participating
        self.stashingWhileCatchingUp = set()       # type: Set[Tuple]

    def shouldParticipate(self, viewNo: int, ppSeqNo: int):
        # Replica should only participating in the consensus process and the
        # replica did not stash any of this request's 3-phase request
        return self.node.isParticipating and (viewNo, ppSeqNo) \
                                             not in self.stashingWhileCatchingUp

    @staticmethod
    def generateName(nodeName: str, instId: int):
        """
        Create and return the name for a replica using its nodeName and
        instanceId.
         Ex: Alpha:1
        """
        return "{}:{}".format(nodeName, instId)

    @staticmethod
    def getNodeName(replicaName: str):
        return replicaName.split(":")[0]

    @property
    def isPrimary(self):
        """
        Is this node primary?

        :return: True if this node is primary, False otherwise
        """
        return self._primaryName == self.name if self._primaryName is not None \
            else None

    @property
    def primaryName(self):
        """
        Name of the primary replica of this replica's instance

        :return: Returns name if primary is known, None otherwise
        """
        return self._primaryName

    @primaryName.setter
    def primaryName(self, value: Optional[str]) -> None:
        """
        Set the value of isPrimary.

        :param value: the value to set isPrimary to
        """
        if not value == self._primaryName:
            self._primaryName = value
            self.primaryNames[self.viewNo] = value
            logger.debug("{} setting primaryName for view no {} to: {}".
                         format(self, self.viewNo, value))
            logger.debug("{}'s primaryNames for views are: {}".
                         format(self, self.primaryNames))
            self._stateChanged()

    def _stateChanged(self):
        """
        A series of actions to be performed when the state of this replica
        changes.

        - UnstashInBox (see _unstashInBox)
        """
        self._unstashInBox()
        if self.isPrimary is not None:
            # TODO handle suspicion exceptions here
            self.process3PhaseReqsQueue()
            # TODO handle suspicion exceptions here
            try:
                self.processPostElectionMsgs()
            except SuspiciousNode as ex:
                self.outBox.append(ex)
                self.discard(ex.msg, ex.reason, logger.warning)

    def _stashInBox(self, msg):
        """
        Stash the specified message into the inBoxStash of this replica.

        :param msg: the message to stash
        """
        self.inBoxStash.append(msg)

    def _unstashInBox(self):
        """
        Append the inBoxStash to the right of the inBox.
        """
        self.inBox.extend(self.inBoxStash)
        self.inBoxStash.clear()

    def __repr__(self):
        return self.name

    @property
    def f(self) -> int:
        """
        Return the number of Byzantine Failures that can be tolerated by this
        system. Equal to (N - 1)/3, where N is the number of nodes in the
        system.
        """
        return self.node.f

    @property
    def viewNo(self):
        """
        Return the current view number of this replica.
        """
        return self.node.viewNo

    def isPrimaryInView(self, viewNo: int) -> Optional[bool]:
        """
        Return whether a primary has been selected for this view number.
        """
        return self.primaryNames[viewNo] == self.name

    def isMsgForLaterView(self, msg):
        """
        Return whether this request's view number is greater than the current
        view number of this replica.
        """
        viewNo = getattr(msg, "viewNo", None)
        return viewNo > self.viewNo

    def isMsgForCurrentView(self, msg):
        """
        Return whether this request's view number is equal to the current view
        number of this replica.
        """
        viewNo = getattr(msg, "viewNo", None)
        return viewNo == self.viewNo

    def isMsgForPrevView(self, msg):
        """
        Return whether this request's view number is less than the current view
        number of this replica.
        """
        viewNo = getattr(msg, "viewNo", None)
        return viewNo < self.viewNo

    def isPrimaryForMsg(self, msg) -> Optional[bool]:
        """
        Return whether this replica is primary if the request's view number is
        equal this replica's view number and primary has been selected for
        the current view.
        Return None otherwise.

        :param msg: message
        """
        if self.isMsgForLaterView(msg):
            self.discard(msg,
                         "Cannot get primary status for a request for a later "
                         "view {}. Request is {}".format(self.viewNo, msg),
                         logger.error)
        else:
            return self.isPrimary if self.isMsgForCurrentView(msg) \
                else self.isPrimaryInView(msg.viewNo)

    def isMsgFromPrimary(self, msg, sender: str) -> bool:
        """
        Return whether this message was from primary replica
        :param msg:
        :param sender:
        :return:
        """
        if self.isMsgForLaterView(msg):
            logger.error("{} cannot get primary for a request for a later "
                         "view. Request is {}".format(self, msg))
        else:
            return self.primaryName == sender if self.isMsgForCurrentView(
                msg) else self.primaryNames[msg.viewNo] == sender

    def _preProcessReqDigest(self, rd: ReqDigest) -> None:
        """
        Process request digest if this replica is not a primary, otherwise stash
        the message into the inBox.

        :param rd: the client Request Digest
        """
        if self.isPrimary is not None:
            self.processReqDigest(rd)
        else:
            logger.debug("{} stashing request digest {} since it does not know "
                         "its primary status".
                         format(self, (rd.identifier, rd.reqId)))
            self._stashInBox(rd)

    def serviceQueues(self, limit=None):
        """
        Process `limit` number of messages in the inBox.

        :param limit: the maximum number of messages to process
        :return: the number of messages successfully processed
        """
        # TODO should handle SuspiciousNode here
        return self.inBoxRouter.handleAllSync(self.inBox, limit)
        # Messages that can be processed right now needs to be added back to the
        # queue. They might be able to be processed later

    def processPostElectionMsgs(self):
        """
        Process messages waiting for the election of a primary replica to
        complete.
        """
        while self.postElectionMsgs:
            msg = self.postElectionMsgs.popleft()
            logger.debug("{} processing pended msg {}".format(self, msg))
            self.dispatchThreePhaseMsg(*msg)

    def process3PhaseReqsQueue(self):
        """
        Process the 3 phase requests from the queue whose view number is equal
        to the current view number of this replica.
        """
        unprocessed = deque()
        while self.threePhaseMsgsForLaterView:
            request, sender = self.threePhaseMsgsForLaterView.popleft()
            logger.debug("{} processing pended 3 phase request: {}"
                         .format(self, request))
            # If the request is for a later view dont try to process it but add
            # it back to the queue.
            if self.isMsgForLaterView(request):
                unprocessed.append((request, sender))
            else:
                self.processThreePhaseMsg(request, sender)
        self.threePhaseMsgsForLaterView = unprocessed

    @property
    def quorum(self) -> int:
        r"""
        Return the quorum of this RBFT system. Equal to :math:`2f + 1`.
        Return None if `f` is not yet determined.
        """
        return self.node.quorum

    def dispatchThreePhaseMsg(self, msg: ThreePhaseMsg, sender: str) -> Any:
        """
        Create a three phase request to be handled by the threePhaseRouter.

        :param msg: the ThreePhaseMsg to dispatch
        :param sender: the name of the node that sent this request
        """
        senderRep = self.generateName(sender, self.instId)
        try:
            self.threePhaseRouter.handleSync((msg, senderRep))
        except SuspiciousNode as ex:
            self.node.reportSuspiciousNodeEx(ex)

    def processReqDigest(self, rd: ReqDigest):
        """
        Process a request digest. Works only if this replica has decided its
        primary status.

        :param rd: the client request digest to process
        """
        self.stats.inc(TPCStat.ReqDigestRcvd)
        if self.isPrimary is False:
            logger.debug("Non primary replica {} pended request for Pre "
                         "Prepare {}".format(self, (rd.identifier, rd.reqId)))
            self.reqsPendingPrePrepare[(rd.identifier, rd.reqId)] = rd.digest
        else:
            self.doPrePrepare(rd)
        if rd.digest in self.commitsPendedForOrdering:
            viewNo, ppSeqNo, ppTime = self.commitsPendedForOrdering[rd.digest]
            self.doOrder(viewNo, ppSeqNo, rd.identifier, rd.reqId, rd.digest,
                         ppTime)
            self.commitsPendedForOrdering.pop((viewNo, ppSeqNo), None)
            self.commitsPendedForOrdering.pop(rd.digest, None)
        
    def processThreePhaseMsg(self, msg: ThreePhaseMsg, sender: str):
        """
        Process a 3-phase (pre-prepare, prepare and commit) request.
        Dispatch the request only if primary has already been decided, otherwise
        stash it.

        :param msg: the Three Phase message, one of PRE-PREPARE, PREPARE,
            COMMIT
        :param sender: name of the node that sent this message
        """
        # Can only proceed further if it knows whether its primary or not
        if self.isMsgForLaterView(msg):
            self.threePhaseMsgsForLaterView.append((msg, sender))
            logger.debug("{} pended received 3 phase request for a later view: "
                         "{}".format(self, msg))
        else:
            if self.isPrimary is None:
                self.postElectionMsgs.append((msg, sender))
                logger.debug("Replica {} pended request {} from {}".
                             format(self, msg, sender))
            else:
                self.dispatchThreePhaseMsg(msg, sender)

    def processPrePrepare(self, pp: PrePrepare, sender: str):
        """
        Validate and process the PRE-PREPARE specified.
        If validation is successful, create a PREPARE and broadcast it.

        :param pp: a prePrepareRequest
        :param sender: name of the node that sent this message
        """
        key = (pp.viewNo, pp.ppSeqNo)
        logger.debug("{} Receiving PRE-PREPARE {} at {} from {}".
                     format(self, key, time.perf_counter(), sender))
        if self.canProcessPrePrepare(pp, sender):
            if not self.node.isParticipating:
                self.stashingWhileCatchingUp.add(key)
            self.addToPrePrepares(pp)
            if key in self.commitsPendedForOrdering or pp.digest in \
                    self.commitsPendedForOrdering:
                self.doOrder(*key, pp.identifier, pp.reqId, pp.digest, pp.ppTime)
                self.commitsPendedForOrdering.pop(key, None)
                self.commitsPendedForOrdering.pop(pp.digest, None)
            # elif pp.digest in self.commitsPendedForOrdering:
            #     self.doOrder(*key, pp.identifier, pp.reqId, pp.digest)
            #     self.commitsPendedForOrdering.pop(pp.digest)
            #     self.commitsPendedForOrdering.pop(key, None)

            logger.info("{} processed incoming PRE-PREPARE {}".
                           format(self, key))

    def tryPrepare(self, pp: PrePrepare):
        """
        Try to send the Prepare message if the PrePrepare message is ready to
        be passed into the Prepare phase.
        """
        if self.canSendPrepare(pp):
            self.doPrepare(pp)
        else:
            logger.debug("{} cannot send PREPARE".format(self))

    def processPrepare(self, prepare: Prepare, sender: str) -> None:
        """
        Validate and process the PREPARE specified.
        If validation is successful, create a COMMIT and broadcast it.

        :param prepare: a PREPARE msg
        :param sender: name of the node that sent the PREPARE
        """
        # TODO move this try/except up higher
        logger.debug("{} received PREPARE {} from {}".
                     format(self, prepare, sender))
        try:
            if self.isValidPrepare(prepare, sender):
                self.addToPrepares(prepare, sender)
                self.stats.inc(TPCStat.PrepareRcvd)
                logger.debug("{} processed incoming PREPARE {}".
                               format(self, (prepare.viewNo, prepare.ppSeqNo)))
            else:
                # TODO let's have isValidPrepare throw an exception that gets
                # handled and possibly logged higher
                logger.warning("{} cannot process incoming PREPARE".
                               format(self))
        except SuspiciousNode as ex:
            self.node.reportSuspiciousNodeEx(ex)

    def processCommit(self, commit: Commit, sender: str) -> None:
        """
        Validate and process the COMMIT specified.
        If validation is successful, return the message to the node.

        :param commit: an incoming COMMIT message
        :param sender: name of the node that sent the COMMIT
        """
        logger.debug("{} received COMMIT {} from {}".
                     format(self, commit, sender))
        if self.isValidCommit(commit, sender):
            self.stats.inc(TPCStat.CommitRcvd)
            self.addToCommits(commit, sender)
            logger.debug("{} processed incoming COMMIT {}".
                           format(self, (commit.viewNo, commit.ppSeqNo)))

    def tryCommit(self, prepare: Prepare):
        """
        Try to commit if the Prepare message is ready to be passed into the
        commit phase.
        """
        if self.canCommit(prepare):
            self.doCommit(prepare)
        else:
            logger.debug("{} not yet able to send COMMIT".format(self))

    def tryOrder(self, commit: Commit):
        """
        Try to order if the Commit message is ready to be ordered.
        """
        canOrder, reason = self.canOrder(commit)
        if canOrder:
            logger.debug("{} returning request to node".format(self))
            self.tryOrdering(commit)
        else:
            logger.trace("{} cannot return request to node: {}".
                         format(self, reason))

    def doPrePrepare(self, reqDigest: ReqDigest) -> None:
        """
        Broadcast a PRE-PREPARE to all the replicas.

        :param reqDigest: a tuple with elements identifier, reqId, and digest
        """
        if not self.node.isParticipating:
            logger.error("Non participating node is attempting PRE-PREPARE. "
                        "This should not happen.")
            return
        self.prePrepareSeqNo += 1
        tm = time.time()*1000
        logger.debug("{} Sending PRE-PREPARE {} at {}".
                     format(self, (self.viewNo, self.prePrepareSeqNo),
                            time.perf_counter()))
        prePrepareReq = PrePrepare(self.instId,
                                   self.viewNo,
                                   self.prePrepareSeqNo,
                                   *reqDigest,
                                   tm)
        self.sentPrePrepares[self.viewNo, self.prePrepareSeqNo] = (reqDigest,
                                                                   tm)
        self.send(prePrepareReq, TPCStat.PrePrepareSent)

    def doPrepare(self, pp: PrePrepare):
        logger.debug("{} Sending PREPARE {} at {}".
                     format(self, (pp.viewNo, pp.ppSeqNo), time.perf_counter()))
        prepare = Prepare(self.instId,
                          pp.viewNo,
                          pp.ppSeqNo,
                          pp.digest,
                          pp.ppTime)
        self.send(prepare, TPCStat.PrepareSent)
        self.addToPrepares(prepare, self.name)

    def doCommit(self, p: Prepare):
        """
        Create a commit message from the given Prepare message and trigger the
        commit phase
        :param p: the prepare message
        """
        logger.debug("{} Sending COMMIT {} at {}".
                     format(self, (p.viewNo, p.ppSeqNo), time.perf_counter()))
        commit = Commit(self.instId,
                        p.viewNo,
                        p.ppSeqNo,
                        p.digest,
                        p.ppTime)
        self.send(commit, TPCStat.CommitSent)
        self.addToCommits(commit, self.name)

    def canProcessPrePrepare(self, pp: PrePrepare, sender: str) -> bool:
        """
        Decide whether this replica is eligible to process a PRE-PREPARE,
        based on the following criteria:

        - this replica is non-primary replica
        - the request isn't in its list of received PRE-PREPAREs
        - the request is waiting to for PRE-PREPARE and the digest value matches

        :param pp: a PRE-PREPARE msg to process
        :param sender: the name of the node that sent the PRE-PREPARE msg
        :return: True if processing is allowed, False otherwise
        """
        # TODO: Check whether it is rejecting PRE-PREPARE from previous view
        # PRE-PREPARE should not be sent from non primary
        if not self.isMsgFromPrimary(pp, sender):
            raise SuspiciousNode(sender, Suspicions.PPR_FRM_NON_PRIMARY, pp)

        # A PRE-PREPARE is being sent to primary
        if self.isPrimaryForMsg(pp) is True:
            raise SuspiciousNode(sender, Suspicions.PPR_TO_PRIMARY, pp)

        # A PRE-PREPARE is sent that has already been received
        if (pp.viewNo, pp.ppSeqNo) in self.prePrepares:
            raise SuspiciousNode(sender, Suspicions.DUPLICATE_PPR_SENT, pp)

        key = (pp.identifier, pp.reqId)

        # A PRE-PREPARE is sent that does not match request digest
        if (key in self.reqsPendingPrePrepare and
                    self.reqsPendingPrePrepare[key] != pp.digest):
            raise SuspiciousNode(sender, Suspicions.PPR_DIGEST_WRONG, pp)

        return True

    def addToPrePrepares(self, pp: PrePrepare) -> None:
        """
        Add the specified PRE-PREPARE to this replica's list of received
        PRE-PREPAREs.

        :param pp: the PRE-PREPARE to add to the list
        """
        self.prePrepares[(pp.viewNo, pp.ppSeqNo)] = \
            ((pp.identifier, pp.reqId, pp.digest), pp.ppTime)
        self.dequeuePrepares(pp.viewNo, pp.ppSeqNo)
        self.stats.inc(TPCStat.PrePrepareRcvd)
        self.tryPrepare(pp)

    def hasPrepared(self, request) -> bool:
        return self.prepares.hasPrepareFrom(request, self.name)

    def canSendPrepare(self, request) -> bool:
        """
        Return whether the request identified by (identifier, requestId) can
        proceed to the Prepare step.

        :param request: any object with identifier and requestId attributes
        """
        return self.shouldParticipate(request.viewNo, request.ppSeqNo) and \
               self.node.requests.canPrepare(request, self.f + 1) and \
               not self.hasPrepared(request)

    def isValidPrepare(self, prepare: Prepare, sender: str) -> bool:
        """
        Return whether the PREPARE specified is valid.

        :param prepare: the PREPARE to validate
        :param sender: the name of the node that sent the PREPARE
        :return: True if PREPARE is valid, False otherwise
        """
        key = (prepare.viewNo, prepare.ppSeqNo)
        primaryStatus = self.isPrimaryForMsg(prepare)

        ppReqs = self.sentPrePrepares if primaryStatus else self.prePrepares

        # If a non primary replica and receiving a PREPARE request before a
        # PRE-PREPARE request, then proceed

        # PREPARE should not be sent from primary
        if self.isMsgFromPrimary(prepare, sender):
            raise SuspiciousNode(sender, Suspicions.PR_FRM_PRIMARY, prepare)

        # If non primary replica
        if primaryStatus is False:
            if self.prepares.hasPrepareFrom(prepare, sender):
                raise SuspiciousNode(sender, Suspicions.DUPLICATE_PR_SENT, prepare)
            # If PRE-PREPARE not received for the PREPARE, might be slow network
            if key not in ppReqs:
                self.enqueuePrepare(prepare, sender)
                return False
            elif prepare.digest != ppReqs[key][0][2]:
                raise SuspiciousNode(sender, Suspicions.PR_DIGEST_WRONG, prepare)
            elif prepare.ppTime != ppReqs[key][1]:
                raise SuspiciousNode(sender, Suspicions.PR_TIME_WRONG,
                                     prepare)
            else:
                return True
        # If primary replica
        else:
            if self.prepares.hasPrepareFrom(prepare, sender):
                raise SuspiciousNode(sender, Suspicions.DUPLICATE_PR_SENT, prepare)
            # If PRE-PREPARE was not sent for this PREPARE, certainly
            # malicious behavior
            elif key not in ppReqs:
                raise SuspiciousNode(sender, Suspicions.UNKNOWN_PR_SENT, prepare)
            elif prepare.digest != ppReqs[key][0][2]:
                raise SuspiciousNode(sender, Suspicions.PR_DIGEST_WRONG, prepare)
            elif prepare.ppTime != ppReqs[key][1]:
                raise SuspiciousNode(sender, Suspicions.PR_TIME_WRONG,
                                     prepare)
            else:
                return True

    def addToPrepares(self, prepare: Prepare, sender: str):
        self.prepares.addVote(prepare, sender)
        self.tryCommit(prepare)

    def hasCommitted(self, request) -> bool:
        return self.commits.hasCommitFrom(ThreePhaseKey(
            request.viewNo, request.ppSeqNo),
            self.name)

    def canCommit(self, prepare: Prepare) -> bool:
        """
        Return whether the specified PREPARE can proceed to the Commit
        step.

        Decision criteria:

        - If this replica has got just 2f PREPARE requests then commit request.
        - If less than 2f PREPARE requests then probably there's no consensus on
            the request; don't commit
        - If more than 2f then already sent COMMIT; don't commit

        :param prepare: the PREPARE
        """
        return self.shouldParticipate(prepare.viewNo, prepare.ppSeqNo) and \
               self.prepares.hasQuorum(prepare, self.f) and \
               not self.hasCommitted(prepare)

    def isValidCommit(self, commit: Commit, sender: str) -> bool:
        """
        Return whether the COMMIT specified is valid.

        :param commit: the COMMIT to validate
        :return: True if `request` is valid, False otherwise
        """
        primaryStatus = self.isPrimaryForMsg(commit)
        ppReqs = self.sentPrePrepares if primaryStatus else self.prePrepares
        key = (commit.viewNo, commit.ppSeqNo)
        if (key not in self.prepares and
                key not in self.preparesWaitingForPrePrepare):
            logger.debug("{} rejecting commit {} due to lack of prepares".
                         format(self, key))
            # raise SuspiciousNode(sender, Suspicions.UNKNOWN_CM_SENT, commit)
            return False
        elif self.commits.hasCommitFrom(commit, sender):
            raise SuspiciousNode(sender, Suspicions.DUPLICATE_CM_SENT, commit)
        elif commit.digest != self.getDigestFromPrepare(*key):
            raise SuspiciousNode(sender, Suspicions.CM_DIGEST_WRONG, commit)
        elif key in ppReqs and commit.ppTime != ppReqs[key][1]:
            raise SuspiciousNode(sender, Suspicions.CM_TIME_WRONG,
                                 commit)
        else:
            return True

    def addToCommits(self, commit: Commit, sender: str):
        """
        Add the specified COMMIT to this replica's list of received
        commit requests.

        :param commit: the COMMIT to add to the list
        :param sender: the name of the node that sent the COMMIT
        """
        self.commits.addVote(commit, sender)
        self.tryOrder(commit)

    def hasOrdered(self, request) -> bool:
        return (request.viewNo, request.ppSeqNo) in self.ordered

    def canOrder(self, commit: Commit) -> Tuple[bool, Optional[str]]:
        """
        Return whether the specified commitRequest can be returned to the node.

        Decision criteria:

        - If have got just 2f+1 Commit requests then return request to node
        - If less than 2f+1 of commit requests then probably don't have
            consensus on the request; don't return request to node
        - If more than 2f+1 then already returned to node; don't return request
            to node

        :param commit: the COMMIT
        """
        if not self.commits.hasQuorum(commit, self.f):
            return False, "no quorum: {} commits where f is {}".\
                          format(commit, self.f)
        if self.hasOrdered(commit):
            return False, "already ordered"
        return True, None

    def tryOrdering(self, commit: Commit) -> None:
        """
        Attempt to send an ORDERED request for the specified COMMIT to the
        node.

        :param commit: the COMMIT message
        """
        key = (commit.viewNo, commit.ppSeqNo)
        logger.debug("{} ordering commit {}".format(self, key))
        primaryStatus = self.isPrimaryForMsg(commit)

        if primaryStatus is True:
            (identifier, reqId, digest), tm = self.sentPrePrepares[key]
        elif primaryStatus is False:
            # When the node received PREPARE requests and PRE-PREPARE request
            if key in self.prePrepares:
                (identifier, reqId, digest), tm = self.prePrepares[key]
            else:
                digest = self.getDigestFromPrepare(*key)
                if digest:
                    for (cid, rid), dgst in self.reqsPendingPrePrepare.items():
                        if digest == dgst:
                            identifier, reqId = cid, rid
                            break
                    else:
                        logger.info("{} cannot find digest {} in "
                                    "reqsPendingPrePrepare for viewNo {} and "
                                    "ppSeqNo {}".format(self, digest, *key))
                        self.commitsPendedForOrdering[digest] = (*key,
                                                                 commit.ppTime)
                        self.commitsPendedForOrdering[key] = commit.ppTime
                        return
                else:
                    logger.info("{} cannot find digest {} in "
                                "received PREPAREs for viewNo {} and "
                                "ppSeqNo {}".format(self, digest, *key))
                    self.commitsPendedForOrdering[key] = commit.ppTime
                    return
        else:
            self.discard(commit,
                         "{}'s primary status found None while returning "
                         "request {} to node".format(self, commit),
                         logger.warning)
            return

        self.doOrder(*key, identifier, reqId, digest, commit.ppTime)

    def doOrder(self, viewNo, ppSeqNo, identifier, reqId, digest, ppTime):
        key = (viewNo, ppSeqNo)
        self.addToOrdered(*key)
        ordered = Ordered(self.instId,
                          viewNo,
                          identifier,
                          reqId,
                          digest,
                          ppTime)
        self.send(ordered, TPCStat.OrderSent)
        if key in self.stashingWhileCatchingUp:
            self.stashingWhileCatchingUp.remove(key)

    def addToOrdered(self, viewNo: int, ppSeqNo: int):
        self.ordered.add((viewNo, ppSeqNo))

    def enqueuePrepare(self, request: Prepare, sender: str):
        logger.debug("Queueing prepares due to unavailability of "
                      "pre-prepare. request {} from {}".format(request, sender))
        key = (request.viewNo, request.ppSeqNo)
        if key not in self.preparesWaitingForPrePrepare:
            self.preparesWaitingForPrePrepare[key] = deque()
        self.preparesWaitingForPrePrepare[key].append((request, sender))

    def dequeuePrepares(self, viewNo: int, ppSeqNo: int):
        key = (viewNo, ppSeqNo)
        if key in self.preparesWaitingForPrePrepare:
            i = 0
            # Keys of pending prepares that will be processed below
            while self.preparesWaitingForPrePrepare[key]:
                prepare, sender = self.preparesWaitingForPrePrepare[
                    key].popleft()
                self.processPrepare(prepare, sender)
                i += 1
            logger.debug("{} processed {} PREPAREs waiting for PRE-PREPARE for"
                         " view no {} and seq no {}".format(self, i, viewNo,
                                        ppSeqNo))

    def getDigestFromPrepare(self, viewNo: int, ppSeqNo: int) -> Optional[str]:
        key = (viewNo, ppSeqNo)
        if key in self.prepares:
            return self.prepares[key].digest
        elif key in self.preparesWaitingForPrePrepare:
            prepare, _ = self.preparesWaitingForPrePrepare[key][0]
            return prepare.digest
        else:
            logger.debug("{} could not find digest for PRE-PREPARE {}"
                         .format(self, key))
            return None

    def send(self, msg, stat) -> None:
        """
        Send a message to the node on which this replica resides.

        :param msg: the message to send
        """
        logger.display("{} sending {}".format(self, msg.__class__.__name__),
                     extra={"cli": True})
        logger.trace("{} sending {}".format(self, msg))
        self.stats.inc(stat)
        self.outBox.append(msg)
