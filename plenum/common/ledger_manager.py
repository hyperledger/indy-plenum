import heapq
import math
import operator
import time
from collections import Callable, Counter
from functools import partial
from random import shuffle
from typing import Any, List, Dict, Tuple
from typing import Optional

from ledger.merkle_verifier import MerkleVerifier
from ledger.util import F
from plenum.common.config_util import getConfig
from plenum.common.constants import POOL_LEDGER_ID, LedgerState, DOMAIN_LEDGER_ID, \
    CONSISTENCY_PROOF, CATCH_UP_PREFIX
from plenum.common.ledger import Ledger
from plenum.common.ledger_info import LedgerInfo
from plenum.common.messages.node_messages import LedgerStatus, CatchupRep, \
    ConsistencyProof, f, CatchupReq
from plenum.common.txn_util import reqToTxn
from plenum.common.util import compare_3PC_keys, SortedDict, mostCommonElement, min_3PC_key
from plenum.server.has_action_queue import HasActionQueue
from plenum.server.quorums import Quorums
from stp_core.common.constants import CONNECTION_PREFIX
from stp_core.common.log import getlogger

logger = getlogger()


class LedgerManager(HasActionQueue):
    def __init__(self,
                 owner,
                 ownedByNode: bool = True,
                 postAllLedgersCaughtUp: Optional[Callable] = None,
                 preCatchupClbk: Optional[Callable] = None,
                 ledger_sync_order: Optional[List] = None):
        # If ledger_sync_order is not provided (is None), it is assumed that
        # `postCatchupCompleteClbk` of the LedgerInfo will be used
        self.owner = owner
        self.ownedByNode = ownedByNode
        self.postAllLedgersCaughtUp = postAllLedgersCaughtUp
        self.preCatchupClbk = preCatchupClbk
        self.ledger_sync_order = ledger_sync_order

        self.config = getConfig()
        # Needs to schedule actions. The owner of the manager has the
        # responsibility of calling its `_serviceActions` method periodically
        HasActionQueue.__init__(self)

        # Holds ledgers of different types with
        # their info like callbacks, state, etc
        self.ledgerRegistry = {}  # type: Dict[int, LedgerInfo]

        # Largest 3 phase key received during catchup.
        # This field is needed to discard any stashed 3PC messages or
        # ordered messages since the transactions part of those messages
        # will be applied when they are received through the catchup process
        self.last_caught_up_3PC = (0, 0)

    def __repr__(self):
        return self.owner.name

    def service(self):
        return self._serviceActions()

    def addLedger(self, iD: int, ledger: Ledger,
                  preCatchupStartClbk: Callable = None,
                  postCatchupStartClbk: Callable = None,
                  preCatchupCompleteClbk: Callable = None,
                  postCatchupCompleteClbk: Callable = None,
                  postTxnAddedToLedgerClbk: Callable = None):

        if iD in self.ledgerRegistry:
            logger.error("{} already present in ledgers "
                         "so cannot replace that ledger".format(iD))
            return

        self.ledgerRegistry[iD] = LedgerInfo(
            iD,
            ledger=ledger,
            preCatchupStartClbk=preCatchupStartClbk,
            postCatchupStartClbk=postCatchupStartClbk,
            preCatchupCompleteClbk=preCatchupCompleteClbk,
            postCatchupCompleteClbk=postCatchupCompleteClbk,
            postTxnAddedToLedgerClbk=postTxnAddedToLedgerClbk,
            verifier=MerkleVerifier(ledger.hasher)
        )

    def request_CPs_if_needed(self, ledgerId):
        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        if ledgerInfo.consistencyProofsTimer is None:
            return

        proofs = ledgerInfo.recvdConsistencyProofs
        # there is no any received ConsistencyProofs
        if not proofs:
            return
        logger.debug("{} requesting consistency "
                     "proofs after timeout".format(self))

        quorum = Quorums(self.owner.totalNodes)
        groupedProofs, null_proofs_count = self._groupConsistencyProofs(proofs)
        if quorum.same_consistency_proof.is_reached(null_proofs_count):
            return
        result = self._latestReliableProof(groupedProofs, ledgerInfo.ledger)
        if not result:
            ledger_id, start, end = self.get_consistency_proof_request_params(
                ledgerId, groupedProofs)
            logger.debug("{} sending consistency proof request: {}".
                         format(self, ledger_id, start, end))
            self.owner.request_msg(CONSISTENCY_PROOF,
                                   {f.LEDGER_ID.nm: ledger_id,
                                    f.SEQ_NO_START.nm: start,
                                    f.SEQ_NO_END.nm: end},
                                   self.nodes_to_request_txns_from)

        ledgerInfo.recvdConsistencyProofs = {}
        ledgerInfo.consistencyProofsTimer = None
        ledgerInfo.recvdCatchupRepliesFrm = {}

    @staticmethod
    def _missing_txns(ledger_info) -> Tuple[bool, int]:
        ledger = ledger_info.ledger
        if ledger_info.catchupReplyTimer is None:
            return False, 0

        end = getattr(ledger_info.catchUpTill, f.SEQ_NO_END.nm)

        catchUpReplies = ledger_info.receivedCatchUpReplies
        total_missing = (end - ledger.size) - len(catchUpReplies)
        return total_missing > 0, total_missing

    def request_txns_if_needed(self, ledgerId):
        ledgerInfo = self.ledgerRegistry.get(ledgerId)
        missing, num_missing = self._missing_txns(ledgerInfo)
        if not missing:
            # `catchupReplyTimer` might not be None
            ledgerInfo.catchupReplyTimer = None
            logger.debug('{} not missing any transactions for ledger {}'.
                         format(self, ledgerId))
            return

        ledger = ledgerInfo.ledger
        start = getattr(ledgerInfo.catchUpTill, f.SEQ_NO_START.nm)
        end = getattr(ledgerInfo.catchUpTill, f.SEQ_NO_END.nm)
        catchUpReplies = ledgerInfo.receivedCatchUpReplies

        logger.debug("{} requesting {} missing transactions "
                     "after timeout".format(self, num_missing))
        eligibleNodes = self.nodes_to_request_txns_from

        if not eligibleNodes:
            # TODO: What if all nodes are blacklisted so `eligibleNodes`
            # is empty? It will lead to divide by 0. This should not happen
            #  but its happening.
            # https://www.pivotaltracker.com/story/show/130602115
            logger.error("{}{} could not find any node to request "
                         "transactions from. Catchup process cannot "
                         "move ahead.".format(CATCH_UP_PREFIX, self))
            return

        # Shuffling order of nodes so that catchup requests don't go to
        # the same nodes. This is done to avoid scenario where a node
        # does not reply at all.
        # TODO: Need some way to detect nodes that are not responding.
        shuffle(eligibleNodes)
        batchSize = math.ceil(num_missing / len(eligibleNodes))
        cReqs = []
        lastSeenSeqNo = ledger.size
        leftMissing = num_missing

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
                missing = addReqsForMissing(lastSeenSeqNo + 1, seqNo - 1)
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
                missing = addReqsForMissing(ledger.size + 1, end)
                leftMissing -= missing
            # did not have transactions till `end`
            elif lastSeenSeqNo != end:
                missing = addReqsForMissing(lastSeenSeqNo + 1, end)
                leftMissing -= missing
            else:
                logger.error("{}{} still missing {} transactions. "
                             "Something happened which was not thought "
                             "of. {} {} {}"
                             .format(CATCH_UP_PREFIX, self, leftMissing,
                                     start, end, lastSeenSeqNo))
            if leftMissing:
                logger.error("{}{} still missing {} transactions. {} {} {}"
                             .format(CATCH_UP_PREFIX, self, leftMissing,
                                     start, end, lastSeenSeqNo))

        numElgNodes = len(eligibleNodes)
        for i, req in enumerate(cReqs):
            nodeName = eligibleNodes[i % numElgNodes]
            self.sendTo(req, nodeName)

        ledgerInfo.catchupReplyTimer = time.perf_counter()
        timeout = int(self._getCatchupTimeout(len(cReqs), batchSize))
        self._schedule(partial(self.request_txns_if_needed, ledgerId), timeout)

    def setLedgerState(self, ledgerType: int, state: LedgerState):
        if ledgerType not in self.ledgerRegistry:
            logger.error("ledger type {} not present in ledgers so "
                         "cannot set state".format(ledgerType))
            return
        self.getLedgerInfoByType(ledgerType).state = state

    def setLedgerCanSync(self, ledgerType: int, canSync: bool):
        try:
            ledger_info = self.getLedgerInfoByType(ledgerType)
            ledger_info.canSync = canSync
        except KeyError:
            logger.error("ledger type {} not present in ledgers so "
                         "cannot set state".format(ledgerType))
            return

    def prepare_ledgers_for_sync(self):
        for ledger_info in self.ledgerRegistry.values():
            ledger_info.set_defaults()

    def processLedgerStatus(self, status: LedgerStatus, frm: str):
        logger.debug("{} received ledger status: {} from {}".
                     format(self, status, frm))
        if not status:
            logger.debug("{} found ledger status to be null from {}".
                         format(self, frm))
            return

        # Nodes might not be using pool txn ledger, might be using simple node
        # registries (old approach)
        ledgerStatus = LedgerStatus(*status)
        if ledgerStatus.txnSeqNo < 0:
            self.discard(status, reason="Received negative sequence number "
                                        "from {}".format(frm),
                         logMethod=logger.warning)
            return
        ledgerId = getattr(status, f.LEDGER_ID.nm)

        # If this is a node's ledger manager and sender of this ledger status
        #  is a client and its pool ledger is same as this node's pool ledger
        # then send the pool ledger status since client wont be receiving the
        # consistency proof in this case:
        statusFromClient = self.getStack(frm) == self.clientstack
        if self.ownedByNode and statusFromClient:
            if ledgerId != POOL_LEDGER_ID:
                logger.debug("{} received inappropriate "
                             "ledger status {} from client {}"
                             .format(self, status, frm))
                return
            if self.isLedgerSame(ledgerStatus):
                ledger_status = self.owner.build_ledger_status(POOL_LEDGER_ID)
                self.sendTo(ledger_status, frm)

        # If a ledger is yet to sync and cannot sync right now,
        # then stash the ledger status to be processed later
        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        if ledgerInfo.state != LedgerState.synced and not ledgerInfo.canSync:
            self.stashLedgerStatus(ledgerId, status, frm)
            return

        # If this manager is owned by a node and this node's ledger is ahead of
        # the received ledger status
        if self.ownedByNode and self.isLedgerNew(ledgerStatus):
            consistencyProof = self.getConsistencyProof(ledgerStatus)
            if not consistencyProof:
                return None
            self.sendTo(consistencyProof, frm)

        if self.isLedgerOld(ledgerStatus):
            # if ledgerInfo.state == LedgerState.synced:
            if ledgerInfo.state != LedgerState.syncing:
                self.setLedgerCanSync(ledgerId, True)
                ledger_status = self.owner.build_ledger_status(ledgerId)
                self.sendTo(ledger_status, frm)
            return

        if statusFromClient:
            return

        # This node's ledger is not older so it will not receive a
        # consistency proof unless the other node processes a transaction
        # post sending this ledger status
        ledgerInfo.recvdConsistencyProofs[frm] = None
        ledgerInfo.ledgerStatusOk.add(frm)

        if self.isLedgerSame(ledgerStatus):
            ledgerInfo.last_txn_3PC_key[frm] = \
                (ledgerStatus.viewNo, ledgerStatus.ppSeqNo)

        if self.has_ledger_status_quorum(
                len(ledgerInfo.ledgerStatusOk), self.owner.totalNodes):
            logger.debug("{} found out from {} that its "
                         "ledger of type {} is latest".
                         format(self, ledgerInfo.ledgerStatusOk, ledgerId))
            if ledgerInfo.state != LedgerState.synced:
                logger.debug('{} found from ledger status {} that it does '
                             'not need catchup'.format(self, ledgerStatus))
                # If this node's ledger is same as the ledger status (which is
                #  also the majority of the pool), then set the last ordered
                # 3PC key
                self.do_pre_catchup(ledgerId)
                # Any state cleanup that is part of pre-catchup should be
                # done
                last_3PC_key = self._get_last_txn_3PC_key(ledgerInfo)
                self.catchupCompleted(ledgerId, last_3PC_key)
            else:
                # Ledger was already synced
                self.mark_ledger_synced(ledgerId)

    def _get_last_txn_3PC_key(self, ledgerInfo):
        quorum = Quorums(self.owner.totalNodes)
        quorumed_3PC_keys = \
            [
                most_common_element
                for most_common_element, freq in
                Counter(ledgerInfo.last_txn_3PC_key.values()).most_common()
                if quorum.ledger_status_last_3PC.is_reached(freq) and
                most_common_element[0] is not None and
                most_common_element[1] is not None
            ]

        if len(quorumed_3PC_keys) == 0:
            return None

        min_quorumed_3PC_key = min_3PC_key(quorumed_3PC_keys)
        return min_quorumed_3PC_key

    @staticmethod
    def has_ledger_status_quorum(leger_status_num, total_nodes):
        quorum = Quorums(total_nodes).ledger_status
        return quorum.is_reached(leger_status_num)

    def processConsistencyProof(self, proof: ConsistencyProof, frm: str):
        logger.debug("{} received consistency proof: {} from {}".
                     format(self, proof, frm))
        ledgerId = getattr(proof, f.LEDGER_ID.nm)
        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        ledgerInfo.recvdConsistencyProofs[frm] = ConsistencyProof(*proof)

        if self.canProcessConsistencyProof(proof):
            canCatchup, catchUpFrm = self.canStartCatchUpProcess(ledgerId)
            if canCatchup:
                self.startCatchUpProcess(ledgerId, catchUpFrm)

    def canProcessConsistencyProof(self, proof: ConsistencyProof) -> bool:
        ledgerId = getattr(proof, f.LEDGER_ID.nm)
        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        if not ledgerInfo.canSync:
            logger.debug("{} cannot process consistency "
                         "proof since canSync is {}"
                         .format(self, ledgerInfo.canSync))
            return False
        if ledgerInfo.state == LedgerState.syncing:
            logger.debug("{} cannot process consistency "
                         "proof since ledger state is {}"
                         .format(self, ledgerInfo.state))
            return False
        if ledgerInfo.state == LedgerState.synced:
            if not self.checkLedgerIsOutOfSync(ledgerInfo):
                logger.debug("{} cannot process consistency "
                             "proof since in state {} and not enough "
                             "CPs received"
                             .format(self, ledgerInfo.state))
                return False
            logger.debug("{} is out of sync (based on CPs {} and total "
                         "node cnt {}) -> updating ledger"
                         " state from {} to {}"
                         .format(self, ledgerInfo.recvdConsistencyProofs,
                                 self.owner.totalNodes,
                                 ledgerInfo.state, LedgerState.not_synced))
            self.setLedgerState(ledgerId, LedgerState.not_synced)
            if ledgerId == DOMAIN_LEDGER_ID and ledgerInfo.preCatchupStartClbk:
                ledgerInfo.preCatchupStartClbk()
            return self.canProcessConsistencyProof(proof)

        start = getattr(proof, f.SEQ_NO_START.nm)
        end = getattr(proof, f.SEQ_NO_END.nm)
        # TODO: Should we discard where start is older than the ledger size
        ledgerSize = ledgerInfo.ledger.size
        if start > ledgerSize:
            self.discard(proof, reason="Start {} is greater than "
                                       "ledger size {}".
                         format(start, ledgerSize),
                         logMethod=logger.warning)
            return False
        if end <= start:
            self.discard(proof, reason="End {} is not greater than "
                                       "start {}".format(end, start),
                         logMethod=logger.warning)
            return False
        return True

    def checkLedgerIsOutOfSync(self, ledgerInfo) -> bool:
        recvdConsProof = ledgerInfo.recvdConsistencyProofs
        # Consider an f value when this node had not been added
        adjustedQuorum = Quorums(self.owner.totalNodes)
        equal_state_proofs = self.__get_equal_state_proofs_count(
            recvdConsProof)
        return not adjustedQuorum.same_consistency_proof.is_reached(
            equal_state_proofs)

    def processCatchupReq(self, req: CatchupReq, frm: str):
        logger.debug("{} received catchup request: {} from {}".
                     format(self, req, frm))
        if not self.ownedByNode:
            self.discard(req, reason="Only node can serve catchup requests",
                         logMethod=logger.warning)
            return

        start = getattr(req, f.SEQ_NO_START.nm)
        end = getattr(req, f.SEQ_NO_END.nm)
        ledger = self.getLedgerForMsg(req)
        if end < start:
            self.discard(req, reason="Invalid range", logMethod=logger.warning)
            return

        ledger_size = ledger.size

        if start > ledger_size:
            self.discard(req, reason="{} not able to service since "
                                     "ledger size is {} and start is {}"
                         .format(self, ledger_size, start),
                         logMethod=logger.debug)
            return

        if req.catchupTill > ledger_size:
            self.discard(req, reason="{} not able to service since "
                                     "ledger size is {} and catchupTill is {}"
                         .format(self, ledger_size, req.catchupTill),
                         logMethod=logger.debug)
            return

        # Adjusting for end greater than ledger size
        if end > ledger_size:
            logger.debug("{} does not have transactions till {} "
                         "so sending only till {}"
                         .format(self, end, ledger_size))
            end = ledger_size

        logger.debug("node {} requested catchup for {} from {} to {}"
                     .format(frm, end - start + 1, start, end))
        logger.debug("{} generating consistency proof: {} from {}".
                     format(self, end, req.catchupTill))
        cons_proof = self._make_consistency_proof(ledger, end, req.catchupTill)
        txns = {}
        for seq_no, txn in ledger.getAllTxn(start, end):
            txns[seq_no] = self.owner.update_txn_with_extra_data(txn)
        sorted_txns = SortedDict(txns)
        rep = CatchupRep(getattr(req, f.LEDGER_ID.nm),
                         sorted_txns,
                         cons_proof)
        message_splitter = self._make_split_for_catchup_rep(ledger, req.catchupTill)
        self.sendTo(msg=rep,
                    to=frm,
                    message_splitter=message_splitter)

    def _make_consistency_proof(self, ledger, end, catchup_till):
        # TODO: make catchup_till optional
        # if catchup_till is None:
        #     catchup_till = ledger.size
        proof = ledger.tree.consistency_proof(end, catchup_till)
        string_proof = [Ledger.hashToStr(p) for p in proof]
        return string_proof

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
        ledger_info = self.getLedgerInfoByType(ledgerId)
        ledger = ledger_info.ledger

        if txns:
            if frm not in ledger_info.recvdCatchupRepliesFrm:
                ledger_info.recvdCatchupRepliesFrm[frm] = []

            ledger_info.recvdCatchupRepliesFrm[frm].append(rep)

            txns_already_rcvd_in_catchup = ledger_info.receivedCatchUpReplies
            # Creating a list of txns sorted on the basis of sequence
            # numbers, but not keeping any duplicates
            logger.debug("{} merging all received catchups".format(self))
            txns_already_rcvd_in_catchup = self._get_merged_catchup_txns(
                txns_already_rcvd_in_catchup, txns)

            logger.debug(
                "{} merged catchups, there are {} of them now, from {} to {}"
                .format(self, len(txns_already_rcvd_in_catchup),
                        txns_already_rcvd_in_catchup[0][0],
                        txns_already_rcvd_in_catchup[-1][0]))

            numProcessed = self._processCatchupReplies(
                ledgerId, ledger, txns_already_rcvd_in_catchup)
            logger.debug(
                "{} processed {} catchup replies with sequence numbers {}"
                .format(self, numProcessed, [seqNo for seqNo, _ in
                                             txns_already_rcvd_in_catchup[
                                                 :numProcessed]]))

            ledger_info.receivedCatchUpReplies = txns_already_rcvd_in_catchup[numProcessed:]

        # This check needs to happen anyway since it might be the case that
        # just before sending requests for catchup, it might have processed
        # some ordered requests which might have removed the need for catchup
        self.mark_catchup_completed_if_possible(ledger_info)

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
        # If `catchUpReplies` has any transaction that has not been applied
        # to the ledger
        catchUpReplies = catchUpReplies[numProcessed:]
        if catchUpReplies:
            seqNo = catchUpReplies[0][0]
            if seqNo - ledger.seqNo == 1:
                result, nodeName, toBeProcessed = self.hasValidCatchupReplies(
                    ledgerId, ledger, seqNo, catchUpReplies)
                if result:
                    ledgerInfo = self.getLedgerInfoByType(ledgerId)
                    for _, txn in catchUpReplies[:toBeProcessed]:
                        self._add_txn(ledgerId, ledger,
                                      ledgerInfo, reqToTxn(txn))
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

    def _add_txn(self, ledgerId, ledger: Ledger, ledgerInfo, txn):
        merkleInfo = ledger.add(self._transform(txn))
        txn[F.seqNo.name] = merkleInfo[F.seqNo.name]
        ledgerInfo.postTxnAddedToLedgerClbk(ledgerId, txn)

    def _removePrcdCatchupReply(self, ledgerId, node, seqNo):
        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        for i, rep in enumerate(ledgerInfo.recvdCatchupRepliesFrm[node]):
            if str(seqNo) in getattr(rep, f.TXNS.nm):
                break
        ledgerInfo.recvdCatchupRepliesFrm[node].pop(i)

    def _transform(self, txn):
        # Certain transactions might need to be
        # transformed to certain format before applying to the ledger
        txn = reqToTxn(txn)
        z = txn if not self.ownedByNode else \
            self.owner.transform_txn_for_ledger(txn)
        return z

    def hasValidCatchupReplies(self, ledgerId, ledger, seqNo, catchUpReplies):
        # Here seqNo has to be the seqNo of first transaction of
        # `catchupReplies`

        # Get the transactions in the catchup reply which has sequence
        # number `seqNo`
        nodeName, catchupReply = self._getCatchupReplyForSeqNo(ledgerId,
                                                               seqNo)
        txns = getattr(catchupReply, f.TXNS.nm)

        # Add only those transaction in the temporary tree from the above
        # batch which are not present in the ledger
        # Integer keys being converted to strings when marshaled to JSON
        txns = [self._transform(txn)
                for s, txn in catchUpReplies[:len(txns)]
                if str(s) in txns]

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
                                tempTree.root_hash, Ledger.strToHash(finalMTH),
                                [Ledger.strToHash(p) for p in proof]))
            verified = verifier.verify_tree_consistency(
                tempTree.tree_size,
                finalSize,
                tempTree.root_hash,
                Ledger.strToHash(finalMTH),
                [Ledger.strToHash(p) for p in proof]
            )

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

                if str(seqNo) in txns:
                    return k, rep

    def mark_catchup_completed_if_possible(self, ledger_info: LedgerInfo):
        """
        Checks if the ledger is caught up to the the sequence number
        specified in the ConsistencyProof, if yes then mark the catchup as
        done for this ledger.
        :param ledger_info:
        :return: True if catchup is done, false otherwise
        """
        if ledger_info.state != LedgerState.synced:
            cp = ledger_info.catchUpTill
            assert cp
            if getattr(cp, f.SEQ_NO_END.nm) <= ledger_info.ledger.size:
                self.catchupCompleted(ledger_info.id, (cp.viewNo, cp.ppSeqNo))
                return True
        return False

    def canProcessCatchupReply(self, catchupReply: CatchupRep) -> List[Tuple]:
        """
        Checks for any duplicates or gaps in txns, returns txns sorted by seq no
        """
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
        adjustedQuorum = Quorums(self.owner.totalNodes)
        if len(recvdConsProof) == adjustedQuorum.f + 1:
            # At least once correct node believes that this node is behind.

            # Start timer that will expire in some time and if till that time
            # enough CPs are not received, then explicitly request CPs
            # from other nodes, see `request_CPs_if_needed`

            ledgerInfo.consistencyProofsTimer = time.perf_counter()
            self._schedule(partial(self.request_CPs_if_needed, ledgerId),
                           self.config.ConsistencyProofsTimeout * (
                               self.owner.totalNodes - 1))
        if adjustedQuorum.consistency_proof.is_reached(len(recvdConsProof)):
            logger.debug("{} deciding on the basis of CPs {} and f {}".
                         format(self, recvdConsProof, adjustedQuorum.f))
            grpdPrf, null_proofs_count = self._groupConsistencyProofs(
                recvdConsProof)
            # If more than f nodes were found to be at the same state then this
            #  node's state is good too
            if adjustedQuorum.same_consistency_proof.is_reached(
                    null_proofs_count):
                return True, None
            result = self._latestReliableProof(grpdPrf,
                                               ledgerInfo.ledger)
            cp = ConsistencyProof(ledgerId, *result) if result else None
            return bool(result), cp

        logger.debug(
            "{} cannot start catchup since received only {} "
            "consistency proofs but need at least {}".format(
                self,
                len(recvdConsProof),
                adjustedQuorum.consistency_proof.value))
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
                key = (
                    getattr(proof, f.VIEW_NO.nm),
                    getattr(proof, f.PP_SEQ_NO.nm),
                    getattr(proof, f.OLD_MERKLE_ROOT.nm),
                    getattr(proof, f.NEW_MERKLE_ROOT.nm),
                    tuple(getattr(proof, f.HASHES.nm))
                )
                recvdPrf[(start, end)][key] = recvdPrf[(start, end)]. \
                    get(key, 0) + 1
            else:
                logger.debug("{} found proof by {} null".format(self,
                                                                nodeName))
                nullProofs += 1
        return recvdPrf, nullProofs

    def _reliableProofs(self, groupedProofs):
        adjustedQuorum = Quorums(self.owner.totalNodes)
        result = {}
        for (start, end), val in groupedProofs.items():
            for (view_no, lastPpSeqNo, oldRoot,
                 newRoot, hashes), count in val.items():
                if adjustedQuorum.same_consistency_proof.is_reached(count):
                    result[(start, end)] = (view_no, lastPpSeqNo, oldRoot,
                                            newRoot, hashes)
                    # There would be only one correct proof for a range of
                    # sequence numbers
                    break
        return result

    def _latestReliableProof(self, groupedProofs, ledger):
        reliableProofs = self._reliableProofs(groupedProofs)
        latest = None
        for (start, end), (view_no, last_pp_seq_no, oldRoot,
                           newRoot, hashes) in reliableProofs.items():
            # TODO: Can we do something where consistency proof's start is older
            #  than the current ledger's size and proof's end is larger
            # than the current ledger size.
            # Ignore if proof's start is not the same as the ledger's end
            if start != ledger.size:
                continue
            if latest is None or latest[1] < end:
                latest = (start, end) + (view_no, last_pp_seq_no,
                                         oldRoot, newRoot, hashes)
        return latest

    def get_consistency_proof_request_params(self, ledgerId, groupedProofs):
        # Choose the consistency proof which occurs median number of times in
        # grouped proofs. Not choosing the highest since some malicious nodes
        # might be sending non-existent sequence numbers and not choosing the
        # lowest since that might not be enough as some nodes must be lagging
        # behind a lot or some malicious nodes might send low sequence numbers.
        proofs = sorted(groupedProofs.items(),
                        key=lambda t: max(t[1].values()))
        ledger = self.getLedgerInfoByType(ledgerId).ledger
        return ledgerId, ledger.size, proofs[len(proofs) // 2][0][1]

    def do_pre_catchup(self, ledger_id):
        if self.preCatchupClbk:
            self.preCatchupClbk(ledger_id)

    def startCatchUpProcess(self, ledgerId: int, proof: ConsistencyProof):
        if ledgerId not in self.ledgerRegistry:
            self.discard(proof, reason="Unknown ledger type {}".
                         format(ledgerId))
            return

        self.do_pre_catchup(ledgerId)
        logger.debug("{} started catching up with consistency proof {}".
                     format(self, proof))

        if proof is None:
            self.catchupCompleted(ledgerId)
            return

        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        ledgerInfo.state = LedgerState.syncing
        ledgerInfo.consistencyProofsTimer = None
        ledgerInfo.recvdConsistencyProofs = {}

        p = ConsistencyProof(*proof)
        ledgerInfo.catchUpTill = p

        if self.mark_catchup_completed_if_possible(ledgerInfo):
            logger.debug('{} found that ledger {} does not need catchup'.
                         format(self, ledgerId))
        else:
            eligible_nodes = self.nodes_to_request_txns_from
            if eligible_nodes:
                reqs = self.getCatchupReqs(p)
                for (req, to) in zip(reqs, eligible_nodes):
                    self.sendTo(req, to)
                if reqs:
                    ledgerInfo.catchupReplyTimer = time.perf_counter()
                    batchSize = getattr(reqs[0], f.SEQ_NO_END.nm) - \
                        getattr(reqs[0], f.SEQ_NO_START.nm) + 1
                    timeout = self._getCatchupTimeout(len(reqs), batchSize)
                    self._schedule(
                        partial(
                            self.request_txns_if_needed,
                            ledgerId),
                        timeout)
            else:
                logger.info('{}{} needs to catchup ledger {} but it has not'
                            ' found any connected nodes'
                            .format(CATCH_UP_PREFIX, self, ledgerId))

    def _getCatchupTimeout(self, numRequest, batchSize):
        return numRequest * self.config.CatchupTransactionsTimeout

    def catchupCompleted(self, ledgerId: int, last_3PC: Optional[Tuple] = None):
        if ledgerId not in self.ledgerRegistry:
            logger.error("{}{} called catchup completed for ledger {}".
                         format(CATCH_UP_PREFIX, self, ledgerId))
            return

        # Since multiple ledger will be caught up and catchups might happen
        # multiple times for a single ledger, the largest seen
        # ppSeqNo needs to be known.
        if last_3PC is not None \
                and compare_3PC_keys(self.last_caught_up_3PC, last_3PC) > 0:
            self.last_caught_up_3PC = last_3PC

        self.mark_ledger_synced(ledgerId)
        self.catchup_next_ledger(ledgerId)

    def mark_ledger_synced(self, ledger_id):
        ledgerInfo = self.getLedgerInfoByType(ledger_id)
        ledgerInfo.done_syncing()
        logger.info("{}{} completed catching up ledger {},"
                    " caught up {} in total"
                    .format(CATCH_UP_PREFIX, self, ledger_id,
                            ledgerInfo.num_txns_caught_up),
                    extra={'cli': True})

        if self.postAllLedgersCaughtUp:
            if all(l.state == LedgerState.synced
                   for l in self.ledgerRegistry.values()):
                self.postAllLedgersCaughtUp()

    def catchup_next_ledger(self, ledger_id):
        if not self.ownedByNode:
            logger.debug('{} not owned by node'.format(self))
            return
        next_ledger_id = self.ledger_to_sync_after(ledger_id)
        if next_ledger_id is not None:
            self.catchup_ledger(next_ledger_id)
        else:
            logger.debug('{} not found any ledger to catchup after {}'
                         .format(self, ledger_id))

    def catchup_ledger(self, ledger_id):
        try:
            ledger_info = self.getLedgerInfoByType(ledger_id)
            ledger_info.set_defaults()
            ledger_info.canSync = True
            self.owner.request_ledger_status_from_nodes(ledger_id)
            self.processStashedLedgerStatuses(ledger_id)
        except KeyError:
            logger.error("ledger type {} not present in ledgers so "
                         "cannot set state".format(ledger_id))
            return

    def ledger_to_sync_after(self, ledger_id) -> Optional[int]:
        if self.ledger_sync_order:
            try:
                idx = self.ledger_sync_order.index(ledger_id)
                if idx < (len(self.ledger_sync_order) - 1):
                    return self.ledger_sync_order[idx + 1]
            except ValueError:
                return None

    def getCatchupReqs(self, consProof: ConsistencyProof):
        # TODO: This needs to be optimised, there needs to be a minimum size
        # of catchup requests so if a node is trying to catchup only 50 txns
        # from 10 nodes, each of thise 10 nodes will servce 5 txns and prepare
        # a consistency proof for other txns. This is bad for the node catching
        #  up as it involves more network traffic and more computation to verify
        # so many consistency proofs and for the node serving catchup reqs. But
        # if the node sent only 2 catchup requests the network traffic greatly
        # reduces and 25 txns can be read of a single chunk probably
        # (if txns dont span across multiple chunks). A practical value of this
        # "minimum size" is some multiple of chunk size of the ledger
        node_count = len(self.nodes_to_request_txns_from)
        if node_count == 0:
            logger.debug('{} did not find any connected to nodes to send '
                         'CatchupReq'.format(self))
            return
        # TODO: Consider setting start to `max(ledger.size, consProof.start)`
        # since ordered requests might have been executed after receiving
        # sufficient ConsProof in `preCatchupClbk`
        start = getattr(consProof, f.SEQ_NO_START.nm)
        end = getattr(consProof, f.SEQ_NO_END.nm)
        ledger_id = getattr(consProof, f.LEDGER_ID.nm)
        return self._generate_catchup_reqs(start, end, ledger_id, node_count)

    @staticmethod
    def _generate_catchup_reqs(start, end, ledger_id, node_count):
        batch_length = math.ceil((end - start) / node_count)
        reqs = []
        s = start + 1
        e = min(s + batch_length - 1, end)
        for i in range(node_count):
            req = CatchupReq(ledger_id, s, e, end)
            reqs.append(req)
            s = e + 1
            e = min(s + batch_length - 1, end)
            if s > end:
                break
        return reqs

    @staticmethod
    def _get_merged_catchup_txns(existing_txns, new_txns):
        """
        Merge any newly received txns during catchup with already received txns
        :param existing_txns:
        :param new_txns:
        :return:
        """
        idx_to_remove = []
        for i, (seq_no, _) in enumerate(existing_txns):
            if seq_no < new_txns[0][0]:
                continue
            if seq_no > new_txns[-1][0]:
                break
            idx_to_remove.append(seq_no - new_txns[0][0])
        for idx in reversed(idx_to_remove):
            new_txns.pop(idx)

        return list(heapq.merge(existing_txns, new_txns,
                                key=operator.itemgetter(0)))

    def getConsistencyProof(self, status: LedgerStatus):
        ledger = self.getLedgerForMsg(status)  # type: Ledger
        ledgerId = getattr(status, f.LEDGER_ID.nm)
        seqNoStart = getattr(status, f.TXN_SEQ_NO.nm)
        seqNoEnd = ledger.size
        return self._buildConsistencyProof(ledgerId, seqNoStart, seqNoEnd)

    @staticmethod
    def __get_equal_state_proofs_count(proofs):
        return sum(1 for frm, proof in proofs.items() if not proof)

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
        if seqNoEnd < seqNoStart:
            self.error(
                '{} cannot build consistency proof since end {} is '
                'lesser than start {}'.format(
                    self, seqNoEnd, seqNoStart))
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
        key = self.owner.three_phase_key_for_txn_seq_no(ledgerId, seqNoEnd)
        logger.debug('{} found 3 phase key {} for ledger {} seqNo {}'.
                     format(self, key, ledgerId, seqNoEnd))
        if key is None:
            # The node receiving consistency proof should check if it has
            # received this sentinel 3 phase key (0, 0) in spite of seeing a
            # non-zero txn seq no
            key = (0, 0)

        return ConsistencyProof(
            ledgerId,
            seqNoStart,
            seqNoEnd,
            *key,
            Ledger.hashToStr(oldRoot),
            Ledger.hashToStr(newRoot),
            [Ledger.hashToStr(p) for p in proof]
        )

    def _compareLedger(self, status: LedgerStatus):
        ledgerId = getattr(status, f.LEDGER_ID.nm)
        seqNo = getattr(status, f.TXN_SEQ_NO.nm)
        ledger = self.getLedgerForMsg(status)
        logger.debug("{} comparing its ledger {} "
                     "of size {} with {}"
                     .format(self, ledgerId, ledger.seqNo, seqNo))
        return ledger.seqNo - seqNo

    def isLedgerOld(self, status: LedgerStatus):
        # Is self ledger older than the `LedgerStatus`
        return self._compareLedger(status) < 0

    def isLedgerNew(self, status: LedgerStatus):
        # Is self ledger newer than the `LedgerStatus`
        return self._compareLedger(status) > 0

    def isLedgerSame(self, status: LedgerStatus):
        # Is self ledger same as the `LedgerStatus`
        return self._compareLedger(status) == 0

    def getLedgerForMsg(self, msg: Any) -> Ledger:
        ledger_id = getattr(msg, f.LEDGER_ID.nm)
        if ledger_id in self.ledgerRegistry:
            return self.getLedgerInfoByType(ledger_id).ledger
        self.discard(msg, reason="Invalid ledger msg type")

    def getLedgerInfoByType(self, ledgerType) -> LedgerInfo:
        if ledgerType not in self.ledgerRegistry:
            raise KeyError("Invalid ledger type: {}".format(ledgerType))
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
        max_iter = len(ledgerInfo.stashedLedgerStatuses)
        logger.debug(
            '{} going to process {} stashed ledger statuses for ledger'
            ' {}'.format(
                self, max_iter, ledgerId))
        # Since `processLedgerStatus` can stash some ledger statuses, make sure
        # each item in `ledgerInfo.stashedLedgerStatuses` is processed only
        # once
        while max_iter != i:
            msg, frm = ledgerInfo.stashedLedgerStatuses.popleft()
            i += 1
            self.processLedgerStatus(msg, frm)
        return i

    def getStack(self, remoteName: str):
        if self.ownedByNode and self.clientstack.hasRemote(remoteName):
            return self.clientstack

        if self.nodestack.hasRemote(remoteName):
            return self.nodestack

        logger.error("{}{} cannot find remote with name {}"
                     .format(CONNECTION_PREFIX, self, remoteName))

    def sendTo(self, msg: Any, to: str, message_splitter=None):
        stack = self.getStack(to)

        # If the message is being sent by a node
        if self.ownedByNode:
            if stack == self.nodestack:
                self.sendToNodes(msg, [to, ], message_splitter)
            if stack == self.clientstack:
                self.owner.transmitToClient(msg, to)
        # If the message is being sent by a client
        else:
            self.sendToNodes(msg, [to, ])

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
    def sendToNodes(self):
        return self.owner.sendToNodes

    @property
    def discard(self):
        return self.owner.discard

    @property
    def blacklistedNodes(self):
        if self.ownedByNode:
            return self.owner.blacklistedNodes
        return set()

    @property
    def nodes_to_request_txns_from(self):
        return [nm for nm in self.nodestack.registry
                if nm not in self.blacklistedNodes and nm != self.nodestack.name]

    def _make_split_for_catchup_rep(self, ledger, initial_seq_no):

        def _split(message):
            txns = list(message.txns.items())
            divider = len(message.txns) // 2
            left = txns[:divider]
            left_last_seq_no = left[-1][0]
            right = txns[divider:]
            right_last_seq_no = right[-1][0]
            left_cons_proof = self._make_consistency_proof(ledger,
                                                           left_last_seq_no,
                                                           initial_seq_no)
            right_cons_proof = self._make_consistency_proof(ledger,
                                                            right_last_seq_no,
                                                            initial_seq_no)
            ledger_id = getattr(message, f.LEDGER_ID.nm)

            left_rep = CatchupRep(ledger_id, SortedDict(left), left_cons_proof)
            right_rep = CatchupRep(ledger_id, SortedDict(right), right_cons_proof)
            return left_rep, right_rep

        return _split
