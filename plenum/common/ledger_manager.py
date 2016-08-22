import heapq
import operator
from base64 import b64encode
from collections import Callable
from collections import deque
from typing import Any, List
from typing import Dict

import math
from typing import Set

from ledger.ledger import Ledger
from ledger.util import F
from plenum.common.exceptions import RemoteNotFound
from plenum.common.startable import LedgerState
from plenum.common.types import LedgerStatus, CatchupRep, ConsistencyProof, f, \
    CatchupReq
from plenum.common.util import getlogger, getMaxFailures

logger = getlogger()


class LedgerManager:
    def __init__(self, owner, ownedByNode: bool=True):
        self.owner = owner
        self.ownedByNode = ownedByNode
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

    def __repr__(self):
        return self.owner.name

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
            "postTxnAddedToLedgerClbk": postTxnAddedToLedgerClbk
        }
        self.stashedLedgerStatuses[typ] = deque()
        self.ledgerStatusOk[typ] = set()
        self.recvdConsistencyProofs[typ] = {}
        self.catchUpTill[typ] = None
        self.receivedCatchUpReplies[typ] = []

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
        if not status:
            logger.debug("{} found ledger status to be null from {}".
                         format(self, frm))
            return
        ledgerType = getattr(status, f.LEDGER_TYPE.nm)

        # If this is a node's ledger manager and sender of this ledger status
        #  is a client and its pool ledger is same as this node's pool ledger
        # then send the pool ledger status since client wont be receiving the
        # consistency proof:
        statusFromClient = self.getStack(frm) == self.clientstack
        if self.ownedByNode and statusFromClient:
            if ledgerType != 0:
                logger.debug("{} received inappropriate ledger status {} from "
                             "client {}".format(self, status, frm))
                return
            else:
                if self.isLedgerSame(ledgerStatus):
                    ledger = self.ledgers[0]["ledger"]
                    ledgerStatus = LedgerStatus(0, ledger.size,
                                                ledger.root_hash)
                    self.sendTo(ledgerStatus, frm)

        # If a ledger is yet to sync and cannot sync right now,
        # then stash the
        # ledger status to be processed later
        if self.ledgers[ledgerType]["state"] != LedgerState.synced and \
                not self.ledgers[ledgerType]["canSync"]:
            self.stashLedgerStatus(ledgerType, status, frm)
            return

        # If this manager is owned by a node and the node's ledger is ahead of
        # the received ledger status
        if self.ownedByNode and self.isLedgerNew(ledgerStatus):
            consistencyProof = self.getConsistencyProof(ledgerStatus)
            # rid = self.nodestack.getRemote(frm).uid
            # self.send(ConsistencyProof(*consistencyProof), rid)
            self.sendTo(consistencyProof, frm)

        if not self.isLedgerOld(ledgerStatus) and not statusFromClient:
            # This node's ledger is not older so it will not receive a
            # consistency proof unless the other node processes a transaction
            # post sending this ledger status
            self.recvdConsistencyProofs[ledgerType][frm] = None
            self.ledgerStatusOk[ledgerType].add(frm)
            if len(self.ledgerStatusOk[ledgerType]) == 2*self.owner.f:
                logger.debug("{} found out from {} that its ledger of type {} "
                             "is latest".
                             format(self, self.ledgerStatusOk[ledgerType],
                                    ledgerType))
                self.catchupCompleted(ledgerType)

    def processConsistencyProof(self, proof: ConsistencyProof, frm: str):
        logger.debug("{} received consistency proof: {} from {}".
                     format(self, proof, frm))
        ledgerType = getattr(proof, f.LEDGER_TYPE.nm)
        if self.canProcessConsistencyProof(ledgerType):
            # TODO: Handle case where a malign node can send a consistency
            # proof for a previous ledger status
            self.recvdConsistencyProofs[ledgerType][frm] = \
                ConsistencyProof(*proof)
            canCatchup, catchUpFrm = self.canStartCatchUpProcess(ledgerType)
            if canCatchup:
                self.startCatchUpProcess(ledgerType, catchUpFrm)

    def canProcessConsistencyProof(self, ledgerType: int) -> bool:
        if self.ledgers[ledgerType]["state"] == LedgerState.not_synced and \
                self.ledgers[ledgerType]["canSync"]:
            return True
        else:
            logger.debug("{} cannot process consistency proof since in state {}"
                         " and canSync is {}".format(
                self, self.ledgers[ledgerType]["state"],
                self.ledgers[ledgerType]["canSync"]))
            return False

    def processCatchupReq(self, req: CatchupReq, frm: str):
        logger.debug("{} received catchup request: {} from {}".
                     format(self, req, frm))
        start, end = getattr(req, f.SEQ_NO_START.nm), \
                     getattr(req, f.SEQ_NO_END.nm)
        ledger = self.getLedgerForMsg(req)
        # TODO: This is very inefficient for long ledgers
        txns = ledger.getAllTxn(start, end)
        logger.debug("{} generating consistency proof: {} from {}".
                     format(self, end, ledger.size))
        consProof = [b64encode(p).decode() for p in
                     ledger.tree.consistency_proof(end, ledger.size)]
        # rid = self.nodestack.getRemote(frm).uid
        # self.send(CatchupRep(getattr(req, f.LEDGER_TYPE.nm), txns, consProof),
        #           rid)
        self.sendTo(msg=CatchupRep(getattr(req, f.LEDGER_TYPE.nm), txns,
                                   consProof), to=frm)

    def processCatchupRep(self, rep: CatchupRep, frm: str):
        logger.debug("{} received catchup reply: {} from {}".
                     format(self, rep, frm))
        ledgerType = getattr(rep, f.LEDGER_TYPE.nm)
        if self.canProcessCatchupReply(ledgerType):
            # Not relying on a node sending txns in order of sequence no
            txns = sorted([(int(s), t) for (s, t) in
                           getattr(rep, f.TXNS.nm).items()],
                          key=operator.itemgetter(0))
            if txns:
                ledger = self.getLedgerForMsg(rep)
                catchUpReplies = self.receivedCatchUpReplies[ledgerType]
                catchUpReplies = list(heapq.merge(catchUpReplies, txns,
                                                  key=operator.itemgetter(0)))
                numProcessed = self.processCatchupReplies(ledgerType, ledger,
                                                          catchUpReplies)
                self.receivedCatchUpReplies[ledgerType] = catchUpReplies[numProcessed:]
                if getattr(self.catchUpTill[ledgerType], f.SEQ_NO_END.nm) == \
                        ledger.size:
                    self.catchUpTill[ledgerType] = None
                    self.catchupCompleted(ledgerType)

    def processCatchupReplies(self, ledgerType, ledger: Ledger,
                              catchUpReplies: List):
        processed = []
        for seqNo, txn in catchUpReplies:
            if seqNo - ledger.seqNo == 1:
                txn.pop(F.seqNo.name, None)
                merkleInfo = ledger.add(txn)
                txn[F.seqNo.name] = merkleInfo[F.seqNo.name]
                self.ledgers[ledgerType]["postTxnAddedToLedgerClbk"](ledgerType,
                                                                     txn)
                processed.append(seqNo)
            else:
                break
        logger.debug("{} processed {} catchup replies with sequence numbers {}"
                     .format(self, len(processed), processed))
        return len(processed)

    def canProcessCatchupReply(self, ledgerType: int) -> bool:
        if self.ledgers[ledgerType]["state"] == LedgerState.syncing:
            return True
        else:
            logger.debug("{} cannot process catchup reply for {} since ledger "
                         "is in state {}".
                         format(self, ledgerType,
                                self.ledgers[ledgerType]["state"]))
            return False

    # ASSUMING NO MALICIOUS NODES
    # Assuming that all nodes have the same state of the system and no node
    # is lagging behind. So if two new nodes are added in quick succession in a
    # high traffic environment, this logic is faulty
    def canStartCatchUpProcess(self, ledgerType: int):
        recvdConsProof = self.recvdConsistencyProofs[ledgerType]
        # Consider an f value when this node was not connected
        adjustedF = getMaxFailures(self.owner.totalNodes - 1)
        if len(recvdConsProof) >= 2*adjustedF:
            logger.debug("{} deciding on the basis of CPs {}".
                         format(self, recvdConsProof))
            result = None
            recvdPrf = {}
            # For the case where the node sending the consistency proof is at
            # the same state as this node
            nullProofs = 0
            for nodeName, proof in recvdConsProof.items():
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
                    if recvdPrf[(start, end)][key] > adjustedF:
                        result = proof
                else:
                    logger.debug("{} found proof by {} null".format(self, nodeName))
                    nullProofs += 1

            if nullProofs == len(recvdConsProof):
                return True, None

            return bool(result), result
        logger.debug("{} cannot start catchup since received only {} "
                     "consistency proofs but need at least {}".
                     format(self, len(recvdConsProof), adjustedF + 1))
        return False, None

    def startCatchUpProcess(self, ledgerType: int, proof: ConsistencyProof):
        logger.debug("{} started catching up with consistency proof {}".
                     format(self, proof))
        if ledgerType not in self.ledgers:
            self.owner.discard(proof, reason="Unknown ledger type {}".
                               format(ledgerType))
            return
        self.recvdConsistencyProofs[ledgerType] = {}
        if proof is not None:
            # if ledgerType == 0:
            #     self.mode = Mode.discovering
            # elif ledgerType == 1:
            #     self.mode = Mode.syncing
            self.ledgers[ledgerType]["state"] = LedgerState.syncing
            p = ConsistencyProof(*proof)
            rids = [self.nodestack.getRemote(nm).uid for nm in
                    self.nodestack.conns]
            for req in zip(self.getCatchupReqs(p), rids):
                self.send(*req)
            self.catchUpTill[getattr(p, f.LEDGER_TYPE.nm)] = p
        else:
            self.catchupCompleted(ledgerType)

    def catchupCompleted(self, ledgerType: int):
        logger.debug("{} completed catching up ledger {}".format(self,
                                                                 ledgerType))
        if ledgerType not in self.ledgers:
            logger.error("{} called catchup completed for ledger {}".
                         format(self, ledgerType))
            return
        self.ledgers[ledgerType]["canSync"] = False
        self.ledgers[ledgerType]["state"] = LedgerState.synced
        self.ledgers[ledgerType]["postCatchupCompleteClbk"]()

    def getCatchupReqs(self, consProof: ConsistencyProof):
        nodeCount = len(self.nodestack.conns)
        start = getattr(consProof, f.SEQ_NO_START.nm)
        end = getattr(consProof, f.SEQ_NO_END.nm)
        batchLength = math.ceil((end-start)/nodeCount)
        reqs = []
        s = start + 1
        e = min(s + batchLength - 1, end)
        for i in range(nodeCount):
            reqs.append(CatchupReq(getattr(consProof, f.LEDGER_TYPE.nm),
                                   s, e))
            s = e + 1
            e = min(s + batchLength - 1, end)
            if s > end:
                break
        return reqs

    def getConsistencyProof(self, status: LedgerStatus):
        ledger = self.getLedgerForMsg(status)    # type: Ledger
        seqNoStart = getattr(status, f.TXN_SEQ_NO.nm)
        seqNoEnd = ledger.size
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
            getattr(status, f.LEDGER_TYPE.nm),
            seqNoStart,
            seqNoEnd,
            b64encode(oldRoot).decode(),
            b64encode(newRoot).decode(),
            [b64encode(p).decode() for p in
             proof]
        )

    def _compareLedger(self, status: LedgerStatus):
        ledgerType = getattr(status, f.LEDGER_TYPE.nm)
        seqNo = getattr(status, f.TXN_SEQ_NO.nm)
        ledger = self.getLedgerForMsg(status)
        logger.debug(
            "{} comparing its ledger {} of size {} with {}".format(self,
                                                                   ledgerType,
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
        typ = getattr(msg, f.LEDGER_TYPE.nm)
        if typ in self.ledgers:
            return self.ledgers[typ]["ledger"]
        else:
            self.owner.discard(msg, reason="Invalid ledger msg type")

    def appendToLedger(self, typ: int, txn: Any) -> Dict:
        if typ not in self.ledgers:
            logger.error("ledger type {} not present in ledgers so cannot add "
                         "txn".format(typ))
            return
        return self.ledgers[typ]["ledger"].append(txn)

    def stashLedgerStatus(self, ledgerType: int, status, frm: str):
        logger.debug("{} stashing ledger status {} from {}".
                     format(self, status, frm))
        self.stashedLedgerStatuses[ledgerType].append((status, frm))

    def processStashedLedgerStatuses(self, ledgerType: int):
        if ledgerType not in self.stashedLedgerStatuses:
            logger.error("{} cannot process ledger of type {}".
                         format(self, ledgerType))
            return 0
        i = 0
        while self.stashedLedgerStatuses[ledgerType]:
            msg, frm = self.stashedLedgerStatuses[ledgerType].pop()
            i += 1
            self.processLedgerStatus(msg, frm)
        logger.debug("{} processed {} stashed ledger statuses".format(self, i))
        return i

    def getStack(self, remoteName: str):
        if self.ownedByNode:
            try:
                self.clientstack.getRemote(remoteName)
                return self.clientstack
            except RemoteNotFound:
                pass
        try:
            self.nodestack.getRemote(remoteName)
            return self.nodestack
        except RemoteNotFound:
            logger.error("{} cannot find remote with name {}".
                         format(self, remoteName))

    def sendTo(self, msg: Any, to: str):
        stack = self.getStack(to)
        if self.ownedByNode:
            if stack == self.nodestack:
                rid = self.nodestack.getRemote(to).uid
                self.send(msg, rid)
            if stack == self.clientstack:
                self.owner.transmitToClient(msg, to)
        else:
            rid = self.nodestack.getRemote(to).uid
            signer = self.owner.getSigner(self.owner.defaultIdentifier)
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
