import heapq
import operator
from base64 import b64encode, b64decode
from collections import Callable
from collections import deque
from copy import copy
from functools import partial
from random import shuffle
from typing import Any, List, Dict, Set, Tuple
import math
from typing import Optional

import time
from ledger.ledger import Ledger
from ledger.merkle_verifier import MerkleVerifier
from ledger.util import F

from plenum.common.startable import LedgerState
from plenum.common.types import LedgerStatus, CatchupRep, ConsistencyProof, f, \
    CatchupReq, ConsProofRequest, POOL_LEDGER_ID
from plenum.common.util import getMaxFailures
from plenum.common.config_util import getConfig
from stp_core.common.log import getlogger
from plenum.server.has_action_queue import HasActionQueue

logger = getlogger()


class LedgerManager(HasActionQueue):
    def __init__(self, owner, ownedByNode: bool=True):
        self.owner = owner
        self.ownedByNode = ownedByNode
        self.config = getConfig()
        # Needs to schedule actions. The owner of the manager has the
        # responsibility of calling its `_serviceActions` method periodically.
        HasActionQueue.__init__(self)

        # Holds ledgers of different types with their info like the ledger
        # object, various callbacks, state (can be synced, is already synced,
        # etc).
        self.ledgers = {}   # type: Dict[int, Dict[str, Any]]

        # Ledger statuses received while the ledger was not ready to be synced
        # (`canSync` was set to False)
        self.stashedLedgerStatuses = {}  # type: Dict[int, deque]

        # Dict of sets with each set corresponding to a ledger
        # Each set tracks which nodes claim that this node's ledger status is ok
        # , if a quorum of nodes (2f+1) say its up to date then mark the catchup
        #  process as completed
        self.ledgerStatusOk = {}        # type: Dict[int, Set]

        # Consistency proofs received in process of catching up.
        # Each element of the dict is the dictionary of consistency proofs
        # received for the ledger. For each dictionary key is the node name and
        # value is a consistency proof.
        self.recvdConsistencyProofs = {}  # type: Dict[int, Dict[str,
        # ConsistencyProof]]

        self.catchUpTill = {}

        # Catchup replies that need to be applied to the ledger. First element
        # of the list is a list of transactions that need to be applied to the
        # pool transaction ledger and the second element is the list of
        # transactions that need to be applied to the domain transaction ledger
        self.receivedCatchUpReplies = {}    # type: Dict[int, List]

        # Keep track of received replies from different senders
        self.recvdCatchupRepliesFrm = {}
        # type: Dict[int, Dict[str, List[CatchupRep]]]

        # Tracks the beginning of consistency proof timer. Timer starts when the
        #  node gets f+1 consistency proofs. If the node is not able to begin
        # the catchup process even after the timer expires then it requests
        # consistency proofs.
        self.consistencyProofsTimers = {}
        # type: Dict[int, Optional[float]]

        # Tracks the beginning of catchup reply timer. Timer starts after the
        #  node sends catchup requests. If the node is not able to finish the
        # the catchup process even after the timer expires then it requests
        # missing transactions.
        self.catchupReplyTimers = {}
        # type: Dict[int, Optional[float]]

    def __repr__(self):
        return self.owner.name

    def service(self):
        return self._serviceActions()

    def addLedger(self, typ: int, ledger: Ledger,
                  preCatchupStartClbk: Callable=None,
                  postCatchupStartClbk: Callable=None,
                  preCatchupCompleteClbk: Callable=None,
                  postCatchupCompleteClbk: Callable=None,
                  postTxnAddedToLedgerClbk: Callable=None):
        if typ in self.ledgers:
            logger.error("{} already present in ledgers so cannot replace that "
                         "ledger".format(typ))
            return
        self.ledgers[typ] = {
            "ledger": ledger,
            "state": LedgerState.not_synced,
            "canSync": False,
            "preCatchupStartClbk": preCatchupStartClbk,
            "postCatchupStartClbk": postCatchupStartClbk,
            "preCatchupCompleteClbk": preCatchupCompleteClbk,
            "postCatchupCompleteClbk": postCatchupCompleteClbk,
            "postTxnAddedToLedgerClbk": postTxnAddedToLedgerClbk,
            "verifier": MerkleVerifier(ledger.hasher)
        }
        self.stashedLedgerStatuses[typ] = deque()
        self.ledgerStatusOk[typ] = set()
        self.recvdConsistencyProofs[typ] = {}
        self.catchUpTill[typ] = None
        self.receivedCatchUpReplies[typ] = []
        self.recvdCatchupRepliesFrm[typ] = {}
        self.consistencyProofsTimers[typ] = None
        self.catchupReplyTimers[typ] = None

    def checkIfCPsNeeded(self, ledgerId):
        if self.consistencyProofsTimers[ledgerId] is not None:
            logger.debug("{} requesting consistency proofs after timeout".format(self))
            adjustedF = getMaxFailures(self.owner.totalNodes - 1)
            recvdConsProof = self.recvdConsistencyProofs[ledgerId]
            grpdPrf, nullProofs = self._groupConsistencyProofs(recvdConsProof)
            if nullProofs > adjustedF:
                return
            result = self._latestReliableProof(grpdPrf,
                                               self.ledgers[ledgerId][
                                                   "ledger"])
            if not result:
                cpReq = self.getConsistencyProofRequest(ledgerId, grpdPrf)
                logger.debug("{} sending consistency proof request: {}".
                             format(self, cpReq))
                self.send(cpReq)

            self.recvdConsistencyProofs[ledgerId] = {}
            self.consistencyProofsTimers[ledgerId] = None
            self.recvdCatchupRepliesFrm[ledgerId] = {}

    def checkIfTxnsNeeded(self, ledgerId):
        if self.catchupReplyTimers[ledgerId] is not None:
            catchupTill = self.catchUpTill[ledgerId]
            start, end = getattr(catchupTill, f.SEQ_NO_START.nm), \
                         getattr(catchupTill, f.SEQ_NO_END.nm)
            ledger = self.ledgers[ledgerId]["ledger"]
            catchUpReplies = self.receivedCatchUpReplies[ledgerId]
            totalMissing = (end - ledger.size) - len(catchUpReplies)

            if totalMissing:
                logger.debug(
                    "{} requesting {} missing transactions after timeout".
                    format(self, totalMissing))
                eligibleNodes = list(self.nodestack.conns -
                                     self.blacklistedNodes)
                # Shuffling order of nodes so that catchup requests dont go to
                # the same nodes. This is done to avoid scenario where a node
                # does not reply at all.
                # TODO: Need some way to detect nodes that are not responding.

                # TODO: What id all nodes are blacklisted so `eligibleNodes`
                # is empty? It will lead to divide by 0. This should not happen
                #  but its happening.
                # https://www.pivotaltracker.com/story/show/130602115
                if not eligibleNodes:
                    logger.error("{} could not find any node to request "
                                 "transactions from. Catchup process cannot "
                                 "move ahead.".format(self))
                    return
                shuffle(eligibleNodes)
                batchSize = math.ceil(totalMissing/len(eligibleNodes))
                cReqs = []
                lastSeenSeqNo = ledger.size
                leftMissing = totalMissing

                def addReqsForMissing(frm, to):
                    # Add Catchup requests for missing transactions. `frm` and
                    # `to` are inclusive
                    missing = to - frm + 1
                    numBatches = math.ceil(missing / batchSize)
                    for i in range(numBatches):
                        s = frm + (i * batchSize)
                        e = min(to, frm + ((i + 1) * batchSize) - 1)
                        req = CatchupReq(ledgerId, s, e, end)
                        logger.debug("{} creating catchup request {} to {} till {}".
                                     format(self, s, e, end))
                        cReqs.append(req)
                    return missing

                for seqNo, txn in catchUpReplies:
                    if (seqNo - lastSeenSeqNo) != 1:
                        missing = addReqsForMissing(lastSeenSeqNo+1, seqNo-1)
                        leftMissing -= missing
                    lastSeenSeqNo = seqNo

                # If still missing some transactions from request has not been
                # sent then either `catchUpReplies` was empty or it did not have
                #  transactions till `end`
                if leftMissing > 0:
                    logger.debug("{} still missing {} transactions after "
                                 "looking at receivedCatchUpReplies".
                                 format(self, leftMissing))
                    # `catchUpReplies` was empty
                    if lastSeenSeqNo == ledger.size:
                        missing = addReqsForMissing(ledger.size+1, end)
                        leftMissing -= missing
                    # did not have transactions till `end`
                    elif lastSeenSeqNo != end:
                        missing = addReqsForMissing(lastSeenSeqNo + 1, end)
                        leftMissing -= missing
                    else:
                        logger.error("{} still missing {} transactions. "
                                     "Something happened which was not thought "
                                     "of. {} {} {}"
                                     .format(self, leftMissing, start, end,
                                             lastSeenSeqNo))
                    if leftMissing:
                        logger.error(
                            "{} still missing {} transactions. {} {} {}"
                                .format(self, leftMissing, start, end,
                                        lastSeenSeqNo))

                numElgNodes = len(eligibleNodes)
                for i, req in enumerate(cReqs):
                    nodeName = eligibleNodes[i%numElgNodes]
                    self.send(req, self.nodestack.getRemote(nodeName).uid)
                self.catchupReplyTimers[ledgerId] = time.perf_counter()
                timeout = self._getCatchupTimeout(len(cReqs), batchSize)
                self._schedule(partial(self.checkIfTxnsNeeded, ledgerId),
                               timeout)
            else:
                self.catchupReplyTimers[ledgerId] = None

    def setLedgerState(self, typ: int, state: LedgerState):
        if typ not in self.ledgers:
            logger.error("ledger type {} not present in ledgers so cannot set "
                         "state".format(typ))
            return
        self.ledgers[typ]["state"] = state

    def setLedgerCanSync(self, typ: int, canSync: bool):
        if typ not in self.ledgers:
            logger.error("ledger type {} not present in ledgers so cannot set "
                         "state".format(typ))
            return
        self.ledgers[typ]["canSync"] = canSync

    def processLedgerStatus(self, status: LedgerStatus, frm: str):
        logger.debug("{} received ledger status: {} from {}".
                     format(self, status, frm))
        # Nodes might not be using pool txn ledger, might be using simple node
        # registries (old approach)
        ledgerStatus = LedgerStatus(*status) if status else None
        if ledgerStatus.txnSeqNo < 0:
            self.discard(status, reason="Received negative sequence number "
                         "from {}".format(frm),
                         logMethod=logger.warn)
        if not status:
            logger.debug("{} found ledger status to be null from {}".
                         format(self, frm))
            return
        ledgerId = getattr(status, f.LEDGER_ID.nm)

        # If this is a node's ledger manager and sender of this ledger status
        #  is a client and its pool ledger is same as this node's pool ledger
        # then send the pool ledger status since client wont be receiving the
        # consistency proof:
        statusFromClient = self.getStack(frm) == self.clientstack
        if self.ownedByNode and statusFromClient:
            if ledgerId != POOL_LEDGER_ID:
                logger.debug("{} received inappropriate ledger status {} from "
                             "client {}".format(self, status, frm))
                return
            else:
                if self.isLedgerSame(ledgerStatus):
                    ledger = self.ledgers[POOL_LEDGER_ID]["ledger"]
                    ledgerStatus = LedgerStatus(POOL_LEDGER_ID, ledger.size,
                                                ledger.root_hash)
                    self.sendTo(ledgerStatus, frm)

        # If a ledger is yet to sync and cannot sync right now,
        # then stash the ledger status to be processed later
        if self.ledgers[ledgerId]["state"] != LedgerState.synced and \
                not self.ledgers[ledgerId]["canSync"]:
            self.stashLedgerStatus(ledgerId, status, frm)
            return

        # If this manager is owned by a node and the node's ledger is ahead of
        # the received ledger status
        if self.ownedByNode and self.isLedgerNew(ledgerStatus):
            consistencyProof = self.getConsistencyProof(ledgerStatus)
            self.sendTo(consistencyProof, frm)

        if not self.isLedgerOld(ledgerStatus) and not statusFromClient:
            # This node's ledger is not older so it will not receive a
            # consistency proof unless the other node processes a transaction
            # post sending this ledger status
            self.recvdConsistencyProofs[ledgerId][frm] = None
            self.ledgerStatusOk[ledgerId].add(frm)
            if len(self.ledgerStatusOk[ledgerId]) == 2*self.owner.f:
                logger.debug("{} found out from {} that its ledger of type {} "
                             "is latest".
                             format(self, self.ledgerStatusOk[ledgerId],
                                    ledgerId))
                if self.ledgers[ledgerId]["state"] != LedgerState.synced:
                    self.catchupCompleted(ledgerId)

    def processConsistencyProof(self, proof: ConsistencyProof, frm: str):
        logger.debug("{} received consistency proof: {} from {}".
                     format(self, proof, frm))
        ledgerId = getattr(proof, f.LEDGER_ID.nm)
        if self.canProcessConsistencyProof(proof):
            self.recvdConsistencyProofs[ledgerId][frm] = \
                ConsistencyProof(*proof)
            canCatchup, catchUpFrm = self.canStartCatchUpProcess(ledgerId)
            if canCatchup:
                self.startCatchUpProcess(ledgerId, catchUpFrm)

    def canProcessConsistencyProof(self, proof: ConsistencyProof) -> bool:
        ledgerId = getattr(proof, f.LEDGER_ID.nm)
        if self.ledgers[ledgerId]["state"] == LedgerState.not_synced and \
                self.ledgers[ledgerId]["canSync"]:
            start, end = getattr(proof, f.SEQ_NO_START.nm), \
                         getattr(proof, f.SEQ_NO_END.nm)
            # TODO: Should we discard where start is older than the ledger size
            ledgerSize = self.ledgers[ledgerId]["ledger"].size
            if start > ledgerSize:
                self.discard(proof, reason="Start {} is greater than "
                                           "ledger size {}".
                             format(start, ledgerSize),
                             logMethod=logger.warn)
                return False
            elif end <= start:
                self.discard(proof, reason="End {} is not greater than "
                                           "start {}".format(end, start),
                             logMethod=logger.warn)
                return False
            else:
                return True
        else:
            logger.debug("{} cannot process consistency proof since in state {}"
                         " and canSync is {}".
                format(self, self.ledgers[ledgerId]["state"],
                self.ledgers[ledgerId]["canSync"]))
            return False

    def processCatchupReq(self, req: CatchupReq, frm: str):
        logger.debug("{} received catchup request: {} from {}".
                     format(self, req, frm))

        start, end = getattr(req, f.SEQ_NO_START.nm), \
                     getattr(req, f.SEQ_NO_END.nm)
        ledger = self.getLedgerForMsg(req)
        if end < start:
            self.discard(req, reason="Invalid range", logMethod=logger.warn)
            return
        if start > ledger.size:
            self.discard(req, reason="{} not able to service since ledger size "
                                     "is {}".format(self, ledger.size),
                         logMethod=logger.debug)
            return

        # Adjusting for end greater than ledger size
        if end > ledger.size:
            logger.debug("{} does not have transactions till {} so sending only"
                         " till {}".format(self, end, ledger.size))
            end = ledger.size

        # TODO: This is very inefficient for long ledgers
        txns = ledger.getAllTxn(start, end)

        logger.debug("node {} requested catchup for {} from {} to {}"
                     .format(frm, end - start+1, start, end))

        logger.debug("{} generating consistency proof: {} from {}".
                     format(self, end, req.catchupTill))
        consProof = [b64encode(p).decode() for p in
                     ledger.tree.consistency_proof(end, req.catchupTill)]
        self.sendTo(msg=CatchupRep(getattr(req, f.LEDGER_ID.nm), txns,
                                   consProof), to=frm)

    def processCatchupRep(self, rep: CatchupRep, frm: str):
        logger.debug("{} received catchup reply from {}: {}".
                     format(self, frm, rep))

        ledgerId = getattr(rep, f.LEDGER_ID.nm)
        txns = self.canProcessCatchupReply(rep)
        txnsNum = len(txns) if txns else 0
        logger.debug("{} found {} transactions in the catchup from {}"
                     .format(self, txnsNum, frm))
        if txns:
            ledger = self.getLedgerForMsg(rep)
            if frm not in self.recvdCatchupRepliesFrm[ledgerId]:
                self.recvdCatchupRepliesFrm[ledgerId][frm] = []
            self.recvdCatchupRepliesFrm[ledgerId][frm].append(rep)
            catchUpReplies = self.receivedCatchUpReplies[ledgerId]
            # Creating a list of txns sorted on the basis of sequence
            # numbers
            logger.debug("{} merging all received catchups".format(self))
            catchUpReplies = list(heapq.merge(catchUpReplies, txns,
                                              key=operator.itemgetter(0)))
            logger.debug(
                "{} merged catchups, there are {} of them now, from {} to {}"
                .format(self, len(catchUpReplies), catchUpReplies[0][0],
                        catchUpReplies[-1][0]))

            numProcessed = self._processCatchupReplies(ledgerId, ledger,
                                                       catchUpReplies)
            logger.debug(
                "{} processed {} catchup replies with sequence numbers {}"
                    .format(self, numProcessed, [seqNo for seqNo, _ in
                                                 catchUpReplies[
                                                 :numProcessed]]))
            self.receivedCatchUpReplies[ledgerId] = \
                catchUpReplies[numProcessed:]
            if getattr(self.catchUpTill[ledgerId], f.SEQ_NO_END.nm) == \
                    ledger.size:
                self.catchUpTill[ledgerId] = None
                self.catchupCompleted(ledgerId)

    def _processCatchupReplies(self, ledgerId, ledger: Ledger,
                               catchUpReplies: List):
        # Removing transactions for sequence numbers are already
        # present in the ledger
        # TODO: Inefficient, should check list in reverse and stop at first
        # match since list is already sorted
        numProcessed = sum(1 for s, _ in catchUpReplies if s <= ledger.size)
        if numProcessed:
            logger.debug("{} found {} already processed transactions in the "
                         "catchup replies".format(self, numProcessed))
        catchUpReplies = catchUpReplies[numProcessed:]
        if catchUpReplies:
            seqNo = catchUpReplies[0][0]
            if seqNo - ledger.seqNo == 1:
                result, nodeName, toBeProcessed = self.hasValidCatchupReplies(
                    ledgerId, ledger, seqNo, catchUpReplies)
                if result:
                    for _, txn in catchUpReplies[:toBeProcessed]:
                        merkleInfo = ledger.add(txn)
                        txn[F.seqNo.name] = merkleInfo[F.seqNo.name]
                        self.ledgers[ledgerId]["postTxnAddedToLedgerClbk"](
                            ledgerId, txn)
                    self._removePrcdCatchupReply(ledgerId, nodeName, seqNo)
                    return numProcessed + toBeProcessed + \
                        self._processCatchupReplies(ledgerId, ledger,
                                                    catchUpReplies[toBeProcessed:])
                else:
                    if self.ownedByNode:
                        self.owner.blacklistNode(nodeName,
                                                 reason="Sent transactions "
                                                        "that could not be "
                                                        "verified")
                        self._removePrcdCatchupReply(ledgerId, nodeName,
                                                     seqNo)
                        # Invalid transactions have to be discarded so letting
                        # the caller know how many txns have to removed from
                        # `self.receivedCatchUpReplies`
                        return numProcessed + toBeProcessed
        return numProcessed

    def _removePrcdCatchupReply(self, ledgerId, node, seqNo):
        for i, rep in enumerate(self.recvdCatchupRepliesFrm[ledgerId][node]):
            if str(seqNo) in getattr(rep, f.TXNS.nm):
                break
        self.recvdCatchupRepliesFrm[ledgerId][node].pop(i)

    def hasValidCatchupReplies(self, ledgerId, ledger, seqNo, catchUpReplies):
        # Here seqNo has to be the seqNo of first transaction of
        # `catchupReplies`

        # Get the batch of transactions in the catchup reply which has sequence
        # number `seqNo`
        nodeName, catchupReply = self._getCatchupReplyForSeqNo(ledgerId,
                                                               seqNo)

        txns = getattr(catchupReply, f.TXNS.nm)
        # Add only those transaction in the temporary tree from the above
        # batch
        # Transfers of odcits in RAET converts integer keys to string
        txns = [txn for s, txn in catchUpReplies[:len(txns)] if str(s) in txns]

        # Creating a temporary tree which will be used to verify consistency
        # proof, by inserting transactions. Duplicating a merkle tree is not
        # expensive since we are using a compact merkle tree.
        tempTree = ledger.treeWithAppliedTxns(txns)

        proof = getattr(catchupReply, f.CONS_PROOF.nm)
        verifier = self.ledgers[ledgerId]["verifier"]
        cp = self.catchUpTill[ledgerId]
        finalSize = getattr(cp, f.SEQ_NO_END.nm)
        finalMTH = getattr(cp, f.NEW_MERKLE_ROOT.nm)
        try:
            logger.debug("{} verifying proof for {}, {}, {}, {}, {}".
                         format(self, tempTree.tree_size, finalSize,
                                tempTree.root_hash, b64decode(finalMTH),
                                [b64decode(p) for p in proof]))
            verified = verifier.verify_tree_consistency(tempTree.tree_size,
                                                        finalSize,
                                                        tempTree.root_hash,
                                                        b64decode(finalMTH),
                                                        [b64decode(p) for p in
                                                         proof])
        except Exception as ex:
            logger.info("{} could not verify catchup reply {} since {}".
                        format(self, catchupReply, ex))
            verified = False
        return bool(verified), nodeName, len(txns)

    def _getCatchupReplyForSeqNo(self, ledgerId, seqNo):
        # This is inefficient if we have large number of nodes but since
        # number of node are always between 60-120, this is ok.
        for k, catchupReps in self.recvdCatchupRepliesFrm[ledgerId].items():
            for rep in catchupReps:
                txns = getattr(rep, f.TXNS.nm)
                # Transfers of odcits in RAET converts integer keys to string
                if str(seqNo) in txns:
                    return k, rep

    def processConsistencyProofReq(self, req: ConsProofRequest, frm: str):
        logger.debug("{} received consistency proof request: {} from {}".
                     format(self, req, frm))
        ledgerId = getattr(req, f.LEDGER_ID.nm)
        seqNoStart = getattr(req, f.SEQ_NO_START.nm)
        seqNoEnd = getattr(req, f.SEQ_NO_END.nm)
        consistencyProof = self._buildConsistencyProof(ledgerId, seqNoStart,
                                                       seqNoEnd)
        # TODO: Build a test for this scenario where a node cannot service a
        # consistency proof request
        if consistencyProof:
            self.sendTo(consistencyProof, frm)

    def canProcessCatchupReply(self, catchupReply: CatchupRep) -> List[Tuple]:
        ledgerId = getattr(catchupReply, f.LEDGER_ID.nm)
        if self.ledgers[ledgerId]["state"] == LedgerState.syncing:
            ledger = self.getLedgerForMsg(catchupReply)
            # Not relying on a node sending txns in order of sequence no
            txns = sorted([(int(s), t) for (s, t) in
                           getattr(catchupReply, f.TXNS.nm).items()],
                          key=operator.itemgetter(0))
            anyNew = any([s > ledger.size for s, _ in txns])
            # The transactions should be contiguous in terms of sequence numbers
            noGapsOrDups = len(txns) == 0 or \
                    (len(txns) == (txns[-1][0] - txns[0][0] + 1))
            if not anyNew:
                self.discard(catchupReply,
                             reason="ledger has size {} and it already contains"
                                    " all transactions in the reply".
                             format(ledger.size), logMethod=logger.info)
            if not noGapsOrDups:
                self.discard(catchupReply,
                             reason="contains duplicates or gaps",
                             logMethod=logger.info)
            if anyNew and noGapsOrDups:
                return txns
        else:
            logger.debug("{} cannot process catchup reply {} since ledger "
                         "is in state {}".
                         format(self, catchupReply,
                                self.ledgers[ledgerId]["state"]))

    # ASSUMING NO MALICIOUS NODES
    # Assuming that all nodes have the same state of the system and no node
    # is lagging behind. So if two new nodes are added in quick succession in a
    # high traffic environment, this logic is faulty
    def canStartCatchUpProcess(self, ledgerId: int):
        recvdConsProof = self.recvdConsistencyProofs[ledgerId]
        # Consider an f value when this node was not connected
        adjustedF = getMaxFailures(self.owner.totalNodes - 1)
        if len(recvdConsProof) == (adjustedF+1):
            self.consistencyProofsTimers[ledgerId] = time.perf_counter()
            self._schedule(partial(self.checkIfCPsNeeded, ledgerId),
                           self.config.ConsistencyProofsTimeout * (
                               self.owner.totalNodes - 1))
        if len(recvdConsProof) > 2*adjustedF:
            logger.debug("{} deciding on the basis of CPs {} and f {}".
                         format(self, recvdConsProof, adjustedF))
            grpdPrf, nullProofs = self._groupConsistencyProofs(recvdConsProof)

            # If more than f nodes were found to be at the same state then this
            #  node's state is good too
            if nullProofs > adjustedF:
                return True, None

            result = self._latestReliableProof(grpdPrf,
                                               self.ledgers[ledgerId]["ledger"])

            return bool(result), (None if not result else ConsistencyProof(
                ledgerId, *result))
        logger.debug("{} cannot start catchup since received only {} "
                     "consistency proofs but need at least {}".
                     format(self, len(recvdConsProof), 2*adjustedF + 1))
        return False, None

    def _groupConsistencyProofs(self, proofs):
        recvdPrf = {}
        # For the case where the other node is at the same state as
        # this node
        nullProofs = 0
        for nodeName, proof in proofs.items():
            if proof:
                start, end = getattr(proof, f.SEQ_NO_START.nm), \
                             getattr(proof, f.SEQ_NO_END.nm)
                if (start, end) not in recvdPrf:
                    recvdPrf[(start, end)] = {}
                key = (getattr(proof, f.OLD_MERKLE_ROOT.nm),
                       getattr(proof, f.NEW_MERKLE_ROOT.nm),
                       tuple(getattr(proof, f.HASHES.nm)))
                recvdPrf[(start, end)][key] = recvdPrf[(start, end)]. \
                                                  get(key, 0) + 1
            else:
                logger.debug("{} found proof by {} null".format(self,
                                                                nodeName))
                nullProofs += 1
        return recvdPrf, nullProofs

    def _reliableProofs(self, groupedProofs):
        adjustedF = getMaxFailures(self.owner.totalNodes - 1)
        result = {}
        for (start, end), val in groupedProofs.items():
            for (oldRoot, newRoot, hashes), count in val.items():
                if count > adjustedF:
                    result[(start, end)] = (oldRoot, newRoot, hashes)
                    # There would be only one correct proof for a range of
                    # sequence numbers
                    break
        return result

    def _latestReliableProof(self, groupedProofs, ledger):
        reliableProofs = self._reliableProofs(groupedProofs)
        latest = None
        for (start, end), (oldRoot, newRoot, hashes) in reliableProofs.items():
            # TODO: Can we do something where consistency proof's start is older
            #  than the current ledger's size and proof's end is larger
            # than the current ledger size.
            # Ignore if proof's start is not the same as the ledger's end
            if start != ledger.size:
                continue
            if latest is None:
                latest = (start, end) + (oldRoot, newRoot, hashes)
            elif latest[1] < end:
                latest = (start, end) + (oldRoot, newRoot, hashes)
        return latest

    def getConsistencyProofRequest(self, ledgerId, groupedProofs):
        # Choose the consistency proof which occurs median number of times in
        # grouped proofs. Not choosing the highest since some malicious nodes
        # might be sending non-existent sequence numbers and not choosing the
        # lowest since that might not be enough as some nodes must be lagging
        # behind a lot or some malicious nodes might send low sequence numbers.
        proofs = sorted(groupedProofs.items(), key=lambda t: max(t[1].values()))
        ledger = self.ledgers[ledgerId]["ledger"]
        return ConsProofRequest(ledgerId, ledger.size, proofs[len(proofs) // 2][0][1])

    def startCatchUpProcess(self, ledgerId: int, proof: ConsistencyProof):
        logger.debug("{} started catching up with consistency proof {}".
                     format(self, proof))
        if ledgerId not in self.ledgers:
            self.discard(proof, reason="Unknown ledger type {}".
                         format(ledgerId))
            return
        self.ledgers[ledgerId]["state"] = LedgerState.syncing
        self.consistencyProofsTimers[ledgerId] = None
        self.recvdConsistencyProofs[ledgerId] = {}
        if proof is not None:
            self.ledgers[ledgerId]["state"] = LedgerState.syncing
            p = ConsistencyProof(*proof)
            rids = [self.nodestack.getRemote(nm).uid for nm in
                    self.nodestack.conns]
            reqs = self.getCatchupReqs(p)
            for req in zip(reqs, rids):
                self.send(*req)
            self.catchUpTill[ledgerId] = p
            if reqs:
                self.catchupReplyTimers[ledgerId] = time.perf_counter()
                timeout = self._getCatchupTimeout(
                    len(reqs),
                    getattr(reqs[0], f.SEQ_NO_END.nm) -
                    getattr(reqs[0], f.SEQ_NO_START.nm) + 1)
                self._schedule(partial(self.checkIfTxnsNeeded, ledgerId),
                               timeout)
        else:
            self.catchupCompleted(ledgerId)

    def _getCatchupTimeout(self, numRequest, batchSize):
        return numRequest * (self.config.CatchupTransactionsTimeout +
                             .1*batchSize)

    def catchupCompleted(self, ledgerId: int):
        self.catchupReplyTimers[ledgerId] = None
        logger.debug("{} completed catching up ledger {}".format(self,
                                                                 ledgerId))
        if ledgerId not in self.ledgers:
            logger.error("{} called catchup completed for ledger {}".
                         format(self, ledgerId))
            return
        self.ledgers[ledgerId]["canSync"] = False
        self.ledgers[ledgerId]["state"] = LedgerState.synced
        self.ledgers[ledgerId]["postCatchupCompleteClbk"]()

    def getCatchupReqs(self, consProof: ConsistencyProof):
        nodeCount = len(self.nodestack.conns)
        start = getattr(consProof, f.SEQ_NO_START.nm)
        end = getattr(consProof, f.SEQ_NO_END.nm)
        batchLength = math.ceil((end-start)/nodeCount)
        reqs = []
        s = start + 1
        e = min(s + batchLength - 1, end)
        for i in range(nodeCount):
            reqs.append(CatchupReq(getattr(consProof, f.LEDGER_ID.nm),
                                   s, e, end))
            s = e + 1
            e = min(s + batchLength - 1, end)
            if s > end:
                break
        return reqs

    def getConsistencyProof(self, status: LedgerStatus):
        ledger = self.getLedgerForMsg(status)    # type: Ledger
        ledgerId = getattr(status, f.LEDGER_ID.nm)
        seqNoStart = getattr(status, f.TXN_SEQ_NO.nm)
        seqNoEnd = ledger.size
        return self._buildConsistencyProof(ledgerId, seqNoStart, seqNoEnd)

    def _buildConsistencyProof(self, ledgerId, seqNoStart, seqNoEnd):
        ledger = self.ledgers[ledgerId]["ledger"]
        ledgerSize = ledger.size
        if seqNoStart > ledgerSize:
            logger.error("{} cannot build consistency proof from {} since its "
                         "ledger size is {}".format(self, seqNoStart,
                                                    ledgerSize))
            return
        if seqNoEnd > ledgerSize:
            logger.error("{} cannot build consistency proof till {} since its "
                         "ledger size is {}".format(self, seqNoEnd, ledgerSize))
            return
        if seqNoStart == 0:
            # Consistency proof for an empty tree cannot exist. Using the root
            # hash now so that the node which is behind can verify that
            # TODO: Make this an empty list
            oldRoot = ledger.tree.root_hash
            proof = [oldRoot, ]
        else:
            proof = ledger.tree.consistency_proof(seqNoStart, seqNoEnd)
            oldRoot = ledger.tree.merkle_tree_hash(0, seqNoStart)
        newRoot = ledger.tree.merkle_tree_hash(0, seqNoEnd)
        return ConsistencyProof(
            ledgerId,
            seqNoStart,
            seqNoEnd,
            b64encode(oldRoot).decode(),
            b64encode(newRoot).decode(),
            [b64encode(p).decode() for p in
             proof]
        )

    def _compareLedger(self, status: LedgerStatus):
        ledgerId = getattr(status, f.LEDGER_ID.nm)
        seqNo = getattr(status, f.TXN_SEQ_NO.nm)
        ledger = self.getLedgerForMsg(status)
        logger.debug(
            "{} comparing its ledger {} of size {} with {}".format(self,
                                                                   ledgerId,
                                                                   ledger.seqNo,
                                                                   seqNo))
        return ledger.seqNo - seqNo

    def isLedgerOld(self, status: LedgerStatus):
        return self._compareLedger(status) < 0

    def isLedgerNew(self, status: LedgerStatus):
        return self._compareLedger(status) > 0

    def isLedgerSame(self, status: LedgerStatus):
        return self._compareLedger(status) == 0

    def getLedgerForMsg(self, msg: Any) -> Ledger:
        typ = getattr(msg, f.LEDGER_ID.nm)
        if typ in self.ledgers:
            return self.ledgers[typ]["ledger"]
        else:
            self.discard(msg, reason="Invalid ledger msg type")

    def appendToLedger(self, ledgerId: int, txn: Any) -> Dict:
        if ledgerId not in self.ledgers:
            logger.error("ledger type {} not present in ledgers so cannot add "
                         "txn".format(ledgerId))
            return
        return self.ledgers[ledgerId]["ledger"].append(txn)

    def stashLedgerStatus(self, ledgerId: int, status, frm: str):
        logger.debug("{} stashing ledger status {} from {}".
                     format(self, status, frm))
        self.stashedLedgerStatuses[ledgerId].append((status, frm))

    def processStashedLedgerStatuses(self, ledgerId: int):
        if ledgerId not in self.stashedLedgerStatuses:
            logger.error("{} cannot process ledger of type {}".
                         format(self, ledgerId))
            return 0
        i = 0
        while self.stashedLedgerStatuses[ledgerId]:
            msg, frm = self.stashedLedgerStatuses[ledgerId].pop()
            i += 1
            self.processLedgerStatus(msg, frm)
        logger.debug("{} processed {} stashed ledger statuses".format(self, i))
        return i

    def getStack(self, remoteName: str):
        if self.ownedByNode:
            if self.clientstack.hasRemote(remoteName):
                return self.clientstack
            else:
                pass

        if self.nodestack.hasRemote(remoteName):
            return self.nodestack
        else:
            logger.error("{} cannot find remote with name {}".
                         format(self, remoteName))

    def sendTo(self, msg: Any, to: str):
        stack = self.getStack(to)
        # If the message is being sent by a node
        if self.ownedByNode:
            if stack == self.nodestack:
                rid = self.nodestack.getRemote(to).uid
                self.send(msg, rid)
            if stack == self.clientstack:
                self.owner.transmitToClient(msg, to)
        # If the message is being sent by a client
        else:
            rid = self.nodestack.getRemote(to).uid
            signer = self.owner.fetchSigner(self.owner.defaultIdentifier)
            self.nodestack.send(msg, rid, signer=signer)

    @property
    def nodestack(self):
        return self.owner.nodestack

    @property
    def clientstack(self):
        if self.ownedByNode:
            return self.owner.clientstack
        else:
            logger.debug("{} trying to get clientstack".format(self))

    @property
    def send(self):
        return self.owner.send

    @property
    def discard(self):
        return self.owner.discard

    @property
    def blacklistedNodes(self):
        if self.ownedByNode:
            return self.owner.blacklistedNodes
        else:
            return set()
