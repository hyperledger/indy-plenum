import heapq
import operator
from base64 import b64encode, b64decode
from collections import Callable
from functools import partial
from random import shuffle
from typing import Any, List, Dict, Tuple
import math
from typing import Optional

import time
from ledger.ledger import Ledger
from ledger.merkle_verifier import MerkleVerifier
from ledger.util import F

from plenum.common.startable import LedgerState
from plenum.common.types import LedgerStatus, CatchupRep, \
    ConsistencyProof, f, CatchupReq, ConsProofRequest
from plenum.common.constants import POOL_LEDGER_ID
from plenum.common.util import getMaxFailures
from plenum.common.config_util import getConfig
from stp_core.common.log import getlogger
from plenum.server.has_action_queue import HasActionQueue
from plenum.common.ledger_info import LedgerInfo

logger = getlogger()


class LedgerManager(HasActionQueue):

    def __init__(self,
                 owner,
                 ownedByNode: bool=True,
                 postAllLedgersCaughtUp:
                 Optional[Callable]=None):

        self.owner = owner
        self.ownedByNode = ownedByNode
        self.postAllLedgersCaughtUp = postAllLedgersCaughtUp
        self.config = getConfig()
        # Needs to schedule actions. The owner of the manager has the
        # responsibility of calling its `_serviceActions` method periodically
        HasActionQueue.__init__(self)

        # Holds ledgers of different types with
        # their info like callbacks, state, etc
        self.ledgerRegistry = {}   # type: Dict[int, LedgerInfo]

        # Largest Pre-Prepare sequence number received during catchup.
        # This field is needed to discard any stashed 3PC messages or
        # ordered messages since the transactions part of those messages
        # will be applied when they are received through the catchup process
        self.lastCaughtUpPpSeqNo = -1

    def __repr__(self):
        return self.owner.name

    def service(self):
        return self._serviceActions()

    def addLedger(self, iD: int, ledger: Ledger,
                  preCatchupStartClbk: Callable=None,
                  postCatchupStartClbk: Callable=None,
                  preCatchupCompleteClbk: Callable=None,
                  postCatchupCompleteClbk: Callable=None,
                  postTxnAddedToLedgerClbk: Callable=None):

        if iD in self.ledgerRegistry:
            logger.error("{} already present in ledgers "
                         "so cannot replace that ledger".format(iD))
            return

        self.ledgerRegistry[iD] = LedgerInfo(
            ledger=ledger,
            state=LedgerState.not_synced,
            canSync=False,
            preCatchupStartClbk=preCatchupStartClbk,
            postCatchupStartClbk=postCatchupStartClbk,
            preCatchupCompleteClbk=preCatchupCompleteClbk,
            postCatchupCompleteClbk=postCatchupCompleteClbk,
            postTxnAddedToLedgerClbk=postTxnAddedToLedgerClbk,
            verifier=MerkleVerifier(ledger.hasher)
        )

    def checkIfCPsNeeded(self, ledgerId):
        # TODO: this one not just checks it also initiates
        # consistency proof exchange process
        # It should be renamed or splat on two different methods

        ledgerInfo = self.getLedgerInfoByType(ledgerId)

        if ledgerInfo.consistencyProofsTimer is None:
            return

        logger.debug("{} requesting consistency "
                     "proofs after timeout".format(self))

        adjustedF = getMaxFailures(self.owner.totalNodes - 1)
        proofs = ledgerInfo.recvdConsistencyProofs
        groupedProofs, nullProofs = self._groupConsistencyProofs(proofs)
        if nullProofs > adjustedF:
            return
        result = self._latestReliableProof(groupedProofs, ledgerInfo.ledger)
        if not result:
            cpReq = self.getConsistencyProofRequest(ledgerId, groupedProofs)
            logger.debug("{} sending consistency proof request: {}".
                         format(self, cpReq))
            self.send(cpReq)
        ledgerInfo.recvdConsistencyProofs = {}
        ledgerInfo.consistencyProofsTimer = None
        ledgerInfo.recvdCatchupRepliesFrm = {}

    def checkIfTxnsNeeded(self, ledgerId):

        ledgerInfo = self.ledgerRegistry.get(ledgerId)
        ledger = ledgerInfo.ledger
        if ledgerInfo.catchupReplyTimer is None:
            return

        start = getattr(ledgerInfo.catchUpTill, f.SEQ_NO_START.nm)
        end = getattr(ledgerInfo.catchUpTill, f.SEQ_NO_END.nm)

        catchUpReplies = ledgerInfo.receivedCatchUpReplies
        totalMissing = (end - ledger.size) - len(catchUpReplies)

        if totalMissing == 0:
            ledgerInfo.catchupReplyTimer = None
            return

        logger.debug("{} requesting {} missing transactions "
                     "after timeout".format(self, totalMissing))
        eligibleNodes = list(self.nodestack.conns -
                             self.blacklistedNodes)

        if not eligibleNodes:
            # TODO: What if all nodes are blacklisted so `eligibleNodes`
            # is empty? It will lead to divide by 0. This should not happen
            #  but its happening.
            # https://www.pivotaltracker.com/story/show/130602115
            logger.error("{} could not find any node to request "
                         "transactions from. Catchup process cannot "
                         "move ahead.".format(self))
            return

        # Shuffling order of nodes so that catchup requests don't go to
        # the same nodes. This is done to avoid scenario where a node
        # does not reply at all.
        # TODO: Need some way to detect nodes that are not responding.
        shuffle(eligibleNodes)
        batchSize = math.ceil(totalMissing/len(eligibleNodes))
        cReqs = []
        lastSeenSeqNo = ledger.size
        leftMissing = totalMissing

        def addReqsForMissing(frm, to):
            # Add Catchup requests for missing transactions.
            # `frm` and `to` are inclusive
            missing = to - frm + 1
            numBatches = int(math.ceil(missing / batchSize))
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
                logger.error("{} still missing {} transactions. {} {} {}"
                             .format(self, leftMissing,
                                     start, end, lastSeenSeqNo))

        numElgNodes = len(eligibleNodes)
        for i, req in enumerate(cReqs):
            nodeName = eligibleNodes[i%numElgNodes]
            self.send(req, self.nodestack.getRemote(nodeName).uid)

        ledgerInfo.catchupReplyTimer = time.perf_counter()
        timeout = int(self._getCatchupTimeout(len(cReqs), batchSize))
        self._schedule(partial(self.checkIfTxnsNeeded, ledgerId), timeout)

    def setLedgerState(self, ledgerType: int, state: LedgerState):
        if ledgerType not in self.ledgerRegistry:
            logger.error("ledger type {} not present in ledgers so "
                         "cannot set state".format(ledgerType))
            return
        self.getLedgerInfoByType(ledgerType).state = state

    def setLedgerCanSync(self, ledgerType: int, canSync: bool):
        if ledgerType not in self.ledgerRegistry:
            logger.error("ledger type {} not present in ledgers so "
                         "cannot set state".format(ledgerType))
            return
        self.getLedgerInfoByType(ledgerType).canSync = canSync

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
                logger.debug("{} received inappropriate "
                             "ledger status {} from client {}"
                             .format(self, status, frm))
                return
            if self.isLedgerSame(ledgerStatus):
                ledgerInfo = self.getLedgerInfoByType(POOL_LEDGER_ID)
                poolLedger = ledgerInfo.ledger
                ledgerStatus = LedgerStatus(POOL_LEDGER_ID,
                                            poolLedger.size,
                                            poolLedger.root_hash)
                self.sendTo(ledgerStatus, frm)

        # If a ledger is yet to sync and cannot sync right now,
        # then stash the ledger status to be processed later
        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        if ledgerInfo.state != LedgerState.synced and not ledgerInfo.canSync:
            self.stashLedgerStatus(ledgerId, status, frm)
            return

        # If this manager is owned by a node and the node's ledger is ahead of
        # the received ledger status
        if self.ownedByNode and self.isLedgerNew(ledgerStatus):
            consistencyProof = self.getConsistencyProof(ledgerStatus)
            self.sendTo(consistencyProof, frm)

        if self.isLedgerOld(ledgerStatus) or statusFromClient:
            return

        # This node's ledger is not older so it will not receive a
        # consistency proof unless the other node processes a transaction
        # post sending this ledger status
        ledgerInfo.recvdConsistencyProofs[frm] = None
        ledgerInfo.ledgerStatusOk.add(frm)
        if len(ledgerInfo.ledgerStatusOk) == 2 * self.owner.f:
            logger.debug("{} found out from {} that its "
                         "ledger of type {} is latest".
                         format(self, ledgerInfo.ledgerStatusOk, ledgerId))
            if ledgerInfo.state != LedgerState.synced:
                self.catchupCompleted(ledgerId)

    def processConsistencyProof(self, proof: ConsistencyProof, frm: str):
        logger.debug("{} received consistency proof: {} from {}".
                     format(self, proof, frm))
        ledgerId = getattr(proof, f.LEDGER_ID.nm)
        if self.canProcessConsistencyProof(proof):
            ledgerInfo = self.getLedgerInfoByType(ledgerId)
            ledgerInfo.recvdConsistencyProofs[frm] = ConsistencyProof(*proof)
            canCatchup, catchUpFrm = self.canStartCatchUpProcess(ledgerId)
            if canCatchup:
                self.startCatchUpProcess(ledgerId, catchUpFrm)

    def canProcessConsistencyProof(self, proof: ConsistencyProof) -> bool:
        ledgerId = getattr(proof, f.LEDGER_ID.nm)
        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        if ledgerInfo.state != LedgerState.not_synced or not ledgerInfo.canSync:
            logger.debug("{} cannot process consistency "
                         "proof since in state {} and canSync is {}"
                         .format(self, ledgerInfo.state, ledgerInfo.canSync))

        start = getattr(proof, f.SEQ_NO_START.nm)
        end = getattr(proof, f.SEQ_NO_END.nm)
        # TODO: Should we discard where start is older than the ledger size
        ledgerSize = ledgerInfo.ledger.size
        if start > ledgerSize:
            self.discard(proof, reason="Start {} is greater than "
                                       "ledger size {}".
                         format(start, ledgerSize),
                         logMethod=logger.warn)
            return False
        if end <= start:
            self.discard(proof, reason="End {} is not greater than "
                                       "start {}".format(end, start),
                         logMethod=logger.warn)
            return False
        return True

    def processCatchupReq(self, req: CatchupReq, frm: str):
        logger.debug("{} received catchup request: {} from {}".
                     format(self, req, frm))

        start = getattr(req, f.SEQ_NO_START.nm)
        end = getattr(req, f.SEQ_NO_END.nm)
        ledger = self.getLedgerForMsg(req)
        if end < start:
            self.discard(req, reason="Invalid range", logMethod=logger.warn)
            return
        if start > ledger.size:
            self.discard(req, reason="{} not able to service since "
                                     "ledger size is {}"
                         .format(self, ledger.size),
                         logMethod=logger.debug)
            return

        # Adjusting for end greater than ledger size
        if end > ledger.size:
            logger.debug("{} does not have transactions till {} "
                         "so sending only till {}"
                         .format(self, end, ledger.size))
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

        txns = self.canProcessCatchupReply(rep)
        txnsNum = len(txns) if txns else 0
        logger.debug("{} found {} transactions in the catchup from {}"
                     .format(self, txnsNum, frm))
        if not txns:
            return

        ledgerId = getattr(rep, f.LEDGER_ID.nm)
        ledger = self.getLedgerInfoByType(ledgerId)

        reallyLedger = self.getLedgerForMsg(rep)

        if frm not in ledger.recvdCatchupRepliesFrm:
            ledger.recvdCatchupRepliesFrm[frm] = []

        ledger.recvdCatchupRepliesFrm[frm].append(rep)

        catchUpReplies = ledger.receivedCatchUpReplies
        # Creating a list of txns sorted on the basis of sequence
        # numbers
        logger.debug("{} merging all received catchups".format(self))
        catchUpReplies = list(heapq.merge(catchUpReplies, txns,
                                          key=operator.itemgetter(0)))
        logger.debug(
            "{} merged catchups, there are {} of them now, from {} to {}"
            .format(self, len(catchUpReplies), catchUpReplies[0][0],
                    catchUpReplies[-1][0]))

        numProcessed = self._processCatchupReplies(ledgerId, reallyLedger,
                                                   catchUpReplies)
        logger.debug(
            "{} processed {} catchup replies with sequence numbers {}"
                .format(self, numProcessed, [seqNo for seqNo, _ in
                                             catchUpReplies[
                                             :numProcessed]]))

        ledger.receivedCatchUpReplies = catchUpReplies[numProcessed:]
        if getattr(ledger.catchUpTill, f.SEQ_NO_END.nm) == reallyLedger.size:
            cp = ledger.catchUpTill
            ledger.catchUpTill = None
            self.catchupCompleted(ledgerId, cp.ppSeqNo)

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
                    ledgerInfo = self.getLedgerInfoByType(ledgerId)
                    for _, txn in catchUpReplies[:toBeProcessed]:
                        merkleInfo = ledger.add(txn)
                        txn[F.seqNo.name] = merkleInfo[F.seqNo.name]
                        ledgerInfo.postTxnAddedToLedgerClbk(ledgerId, txn)
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
        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        for i, rep in enumerate(ledgerInfo.recvdCatchupRepliesFrm[node]):
            if str(seqNo) in getattr(rep, f.TXNS.nm):
                break
        ledgerInfo.recvdCatchupRepliesFrm[node].pop(i)

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


        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        verifier = ledgerInfo.verifier
        cp = ledgerInfo.catchUpTill
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

        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        for k, catchupReps in ledgerInfo.recvdCatchupRepliesFrm.items():
            for rep in catchupReps:
                txns = getattr(rep, f.TXNS.nm)
                # Transfers of odcits in RAET converts integer keys to string
                if str(seqNo) in txns:
                    return k, rep

    def processConsistencyProofReq(self, req: ConsProofRequest, frm: str):
        logger.debug("{} received consistency proof request: {} from {}".
                     format(self, req, frm))
        if not self.ownedByNode:
            self.discard(req,
                         reason='this ledger manager is not owned by node',
                         logMethod=logger.warning)
            return
        ledgerId = getattr(req, f.LEDGER_ID.nm)
        seqNoStart = getattr(req, f.SEQ_NO_START.nm)
        seqNoEnd = getattr(req, f.SEQ_NO_END.nm)
        consistencyProof = self._buildConsistencyProof(ledgerId,
                                                       seqNoStart,
                                                       seqNoEnd)
        # TODO: Build a test for this scenario where a node cannot service a
        # consistency proof request
        if consistencyProof:
            self.sendTo(consistencyProof, frm)

    def canProcessCatchupReply(self, catchupReply: CatchupRep) -> List[Tuple]:
        ledgerId = getattr(catchupReply, f.LEDGER_ID.nm)
        ledgerState = self.getLedgerInfoByType(ledgerId).state
        if ledgerState != LedgerState.syncing:
            logger.debug("{} cannot process catchup reply {} since ledger "
                         "is in state {}".
                         format(self, catchupReply, ledgerState))
            return []

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


    # ASSUMING NO MALICIOUS NODES
    # Assuming that all nodes have the same state of the system and no node
    # is lagging behind. So if two new nodes are added in quick succession in a
    # high traffic environment, this logic is faulty
    def canStartCatchUpProcess(self, ledgerId: int):
        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        recvdConsProof = ledgerInfo.recvdConsistencyProofs
        # Consider an f value when this node was not connected
        adjustedF = getMaxFailures(self.owner.totalNodes - 1)
        if len(recvdConsProof) == (adjustedF+1):
            # At least once correct node believes that this node is behind.

            # Start timer that will expire in some time and if till that time
            # enough CPs are not received, then explicitly request CPs
            # from other nodes, see `checkIfCPsNeeded`

            ledgerInfo.consistencyProofsTimer = time.perf_counter()
            self._schedule(partial(self.checkIfCPsNeeded, ledgerId),
                           self.config.ConsistencyProofsTimeout * (
                               self.owner.totalNodes - 1))
        if len(recvdConsProof) > 2 * adjustedF:
            logger.debug("{} deciding on the basis of CPs {} and f {}".
                         format(self, recvdConsProof, adjustedF))
            grpdPrf, nullProofs = self._groupConsistencyProofs(recvdConsProof)
            # If more than f nodes were found to be at the same state then this
            #  node's state is good too
            if nullProofs > adjustedF:
                return True, None
            result = self._latestReliableProof(grpdPrf,
                                               ledgerInfo.ledger)
            cp = ConsistencyProof(ledgerId, *result) if result else None
            return bool(result),cp

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
                key = (getattr(proof, f.PP_SEQ_NO.nm),
                       getattr(proof, f.OLD_MERKLE_ROOT.nm),
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
            for (lastPpSeqNo, oldRoot, newRoot, hashes), count in val.items():
                if count > adjustedF:
                    result[(start, end)] = (lastPpSeqNo, oldRoot, newRoot,
                                            hashes)
                    # There would be only one correct proof for a range of
                    # sequence numbers
                    break
        return result

    def _latestReliableProof(self, groupedProofs, ledger):
        reliableProofs = self._reliableProofs(groupedProofs)
        latest = None
        for (start, end), (lastPpSeqNo, oldRoot, newRoot, hashes) in \
                reliableProofs.items():
            # TODO: Can we do something where consistency proof's start is older
            #  than the current ledger's size and proof's end is larger
            # than the current ledger size.
            # Ignore if proof's start is not the same as the ledger's end
            if start != ledger.size:
                continue
            if latest is None:
                latest = (start, end) + (lastPpSeqNo, oldRoot, newRoot, hashes)
            elif latest[1] < end:
                latest = (start, end) + (lastPpSeqNo, oldRoot, newRoot, hashes)
        return latest

    def getConsistencyProofRequest(self, ledgerId, groupedProofs):
        # Choose the consistency proof which occurs median number of times in
        # grouped proofs. Not choosing the highest since some malicious nodes
        # might be sending non-existent sequence numbers and not choosing the
        # lowest since that might not be enough as some nodes must be lagging
        # behind a lot or some malicious nodes might send low sequence numbers.
        proofs = sorted(groupedProofs.items(), key=lambda t: max(t[1].values()))
        ledger = self.getLedgerInfoByType(ledgerId).ledger
        return ConsProofRequest(ledgerId,
                                ledger.size,
                                proofs[len(proofs) // 2][0][1])

    def startCatchUpProcess(self, ledgerId: int, proof: ConsistencyProof):
        logger.debug("{} started catching up with consistency proof {}".
                     format(self, proof))
        if ledgerId not in self.ledgerRegistry:
            self.discard(proof, reason="Unknown ledger type {}".
                         format(ledgerId))
            return

        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        ledgerInfo.state = LedgerState.syncing
        ledgerInfo.consistencyProofsTimer = None
        ledgerInfo.recvdConsistencyProofs = {}

        if proof is None:
            self.catchupCompleted(ledgerId)
            return

        ledgerInfo.state = LedgerState.syncing
        p = ConsistencyProof(*proof)
        rids = [self.nodestack.getRemote(nm).uid for nm in
                self.nodestack.conns]
        reqs = self.getCatchupReqs(p)
        for req in zip(reqs, rids):
            self.send(*req)
        ledgerInfo.catchUpTill = p
        if reqs:
            ledgerInfo.catchupReplyTimer = time.perf_counter()
            batchSize = getattr(reqs[0], f.SEQ_NO_END.nm) - \
                        getattr(reqs[0], f.SEQ_NO_START.nm) + 1
            timeout = self._getCatchupTimeout(len(reqs), batchSize)
            self._schedule(partial(self.checkIfTxnsNeeded, ledgerId),
                           timeout)

    def _getCatchupTimeout(self, numRequest, batchSize):
        return numRequest * (self.config.CatchupTransactionsTimeout +
                             0.1 * batchSize)

    def catchupCompleted(self, ledgerId: int, lastPpSeqNo: int=-1):
        if self.lastCaughtUpPpSeqNo < lastPpSeqNo:
            self.lastCaughtUpPpSeqNo = lastPpSeqNo

        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        ledgerInfo.catchupReplyTimer = None
        logger.debug("{} completed catching up ledger {}"
                     .format(self, ledgerId))
        if ledgerId not in self.ledgerRegistry:
            logger.error("{} called catchup completed for ledger {}".
                         format(self, ledgerId))
            return

        ledgerInfo.canSync = False
        ledgerInfo.state = LedgerState.synced
        ledgerInfo.postCatchupCompleteClbk()

        if self.postAllLedgersCaughtUp:
            if all(l.state == LedgerState.synced
                   for l in self.ledgerRegistry.values()):
                self.postAllLedgersCaughtUp()

    def getCatchupReqs(self, consProof: ConsistencyProof):
        nodeCount = len(self.nodestack.conns)
        start = getattr(consProof, f.SEQ_NO_START.nm)
        end = getattr(consProof, f.SEQ_NO_END.nm)
        batchLength = math.ceil((end-start)/nodeCount)
        reqs = []
        s = start + 1
        e = min(s + batchLength - 1, end)
        for i in range(nodeCount):
            req = CatchupReq(getattr(consProof, f.LEDGER_ID.nm), s, e, end)
            reqs.append(req)
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

        ledger = self.getLedgerInfoByType(ledgerId).ledger

        ledgerSize = ledger.size
        if seqNoStart > ledgerSize:
            logger.error("{} cannot build consistency proof from {} "
                         "since its ledger size is {}"
                         .format(self, seqNoStart, ledgerSize))
            return
        if seqNoEnd > ledgerSize:
            logger.error("{} cannot build consistency "
                         "proof till {} since its ledger size is {}"
                         .format(self, seqNoEnd, ledgerSize))
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
        ppSeqNo = self.owner.ppSeqNoForTxnSeqNo(ledgerId, seqNoEnd)
        logger.debug('{} found ppSeqNo {} for ledger {} seqNo {}'.
                     format(self, ppSeqNo, ledgerId, seqNoEnd))
        return ConsistencyProof(
            ledgerId,
            seqNoStart,
            seqNoEnd,
            ppSeqNo,
            b64encode(oldRoot).decode(),
            b64encode(newRoot).decode(),
            [b64encode(p).decode() for p in
             proof]
        )

    def _compareLedger(self, status: LedgerStatus):
        ledgerId = getattr(status, f.LEDGER_ID.nm)
        seqNo = getattr(status, f.TXN_SEQ_NO.nm)
        ledger = self.getLedgerForMsg(status)
        logger.debug("{} comparing its ledger {} "
                     "of size {} with {}"
                     .format(self,ledgerId, ledger.seqNo, seqNo))
        return ledger.seqNo - seqNo

    def isLedgerOld(self, status: LedgerStatus):
        return self._compareLedger(status) < 0

    def isLedgerNew(self, status: LedgerStatus):
        return self._compareLedger(status) > 0

    def isLedgerSame(self, status: LedgerStatus):
        return self._compareLedger(status) == 0

    def getLedgerForMsg(self, msg: Any) -> Ledger:
        ledgerType = getattr(msg, f.LEDGER_ID.nm)
        if ledgerType in self.ledgerRegistry:
            return self.getLedgerInfoByType(ledgerType).ledger
        self.discard(msg, reason="Invalid ledger msg type")

    def getLedgerInfoByType(self, ledgerType) -> LedgerInfo:
        if ledgerType not in self.ledgerRegistry:
            raise ValueError("Invalid ledger type: {}".format(ledgerType))
        return self.ledgerRegistry[ledgerType]

    def appendToLedger(self, ledgerId: int, txn: Any) -> Dict:
        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        return ledgerInfo.ledger.append(txn)

    def stashLedgerStatus(self, ledgerId: int, status, frm: str):
        logger.debug("{} stashing ledger status {} from {}".
                     format(self, status, frm))
        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        ledgerInfo.stashedLedgerStatuses.append((status, frm))

    def processStashedLedgerStatuses(self, ledgerId: int):
        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        i = 0
        while ledgerInfo.stashedLedgerStatuses:
            msg, frm = ledgerInfo.stashedLedgerStatuses.pop()
            i += 1
            self.processLedgerStatus(msg, frm)
        logger.debug("{} processed {} stashed ledger statuses".format(self, i))
        return i

    def getStack(self, remoteName: str):
        if self.ownedByNode and self.clientstack.hasRemote(remoteName):
            return self.clientstack

        if self.nodestack.hasRemote(remoteName):
            return self.nodestack

        logger.error("{} cannot find remote with name {}"
                     .format(self, remoteName))

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
        return self.owner.clientstack if self.ownedByNode else None

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
        return set()
