import random
import types
from functools import partial

import common.error
import plenum.common.error
from plenum.common.types import f

from plenum.common.messages.node_messages import ViewChangeDone, Nomination, Batch, Reelection, \
    Primary, BlacklistMsg, RequestAck, RequestNack, Reject, PoolLedgerTxns, Ordered, \
    Propagate, PrePrepare, Prepare, Commit, Checkpoint, ThreePCState, CheckpointState, \
    Reply, InstanceChange, LedgerStatus, ConsistencyProof, CatchupReq, CatchupRep, ViewChangeDone, \
    CurrentState, MessageReq, MessageRep, ElectionType, ThreePhaseType, ThreePhaseMsg
from plenum.common.request import Request

from plenum.common.util import updateNamedTuple
from stp_core.common.log import getlogger
from plenum.server.replica import TPCStat
from plenum.test.test_node import TestNode, TestReplica, getPrimaryReplica, \
    getNonPrimaryReplicas
from plenum.test.delayers import ppDelay, cDelay

logger = getlogger()


def makeNodeFaulty(node, *behaviors):
    for behavior in behaviors:
        behavior(node)


def changesRequest(node):
    def evilCreatePropagate(self,
                            request: Request, identifier: str) -> Propagate:
        logger.debug("EVIL: Creating propagate request for client request {}".
                     format(request))
        request.operation["amount"] += random.random()
        request._digest = request.getDigest()
        if isinstance(identifier, bytes):
            identifier = identifier.decode()
        return Propagate(request.as_dict, identifier)

    evilMethod = types.MethodType(evilCreatePropagate, node)
    node.createPropagate = evilMethod
    return node


def delaysPrePrepareProcessing(node, delay: float=30, instId: int=None):
    node.nodeIbStasher.delay(ppDelay(delay=delay, instId=instId))


def delaysCommitProcessing(node, delay: float=30, instId: int=None):
    node.nodeIbStasher.delay(cDelay(delay=delay, instId=instId))


# Could have this method directly take a replica rather than a node and an
# instance id but this looks more useful as a complete node can be malicious
def sendDuplicate3PhaseMsg(
        node: TestNode,
        msgType: ThreePhaseMsg,
        count: int=2,
        instId=None):
    def evilSendPrePrepareRequest(self, ppReq: PrePrepare):
        logger.debug("EVIL: Sending duplicate pre-prepare message: {}".
                     format(ppReq))
        self.sentPrePrepares[self.viewNo, self.lastPrePrepareSeqNo] = ppReq
        sendDup(self, ppReq, TPCStat.PrePrepareSent, count)

    def evilSendPrepare(self, ppReq: PrePrepare):
        prepare = Prepare(self.instId,
                          ppReq.viewNo,
                          ppReq.ppSeqNo,
                          ppReq.ppTime,
                          ppReq.digest,
                          ppReq.stateRootHash,
                          ppReq.txnRootHash)
        logger.debug("EVIL: Creating prepare message for request {}: {}".
                     format(ppReq, prepare))
        self.addToPrepares(prepare, self.name)
        sendDup(self, prepare, TPCStat.PrepareSent, count)

    def evilSendCommit(self, request):
        commit = Commit(self.instId,
                        request.viewNo,
                        request.ppSeqNo)
        logger.debug("EVIL: Creating commit message for request {}: {}".
                     format(request, commit))
        self.addToCommits(commit, self.name)
        sendDup(self, commit, TPCStat.CommitSent, count)

    def sendDup(sender, msg, stat, count: int):
        for i in range(count):
            sender.send(msg, stat)

    methodMap = {
        PrePrepare: evilSendPrePrepareRequest,
        Prepare: evilSendPrepare,
        Commit: evilSendCommit
    }

    malMethod = partial(malign3PhaseSendingMethod, msgType=msgType,
                        evilMethod=methodMap[msgType])

    malignInstancesOfNode(node, malMethod, instId)
    return node


def malign3PhaseSendingMethod(replica: TestReplica, msgType: ThreePhaseMsg,
                              evilMethod):
    evilMethod = types.MethodType(evilMethod, replica)

    if msgType == PrePrepare:
        replica.sendPrePrepare = evilMethod
    elif msgType == Prepare:
        replica.doPrepare = evilMethod
    elif msgType == Commit:
        replica.doCommit = evilMethod
    else:
        common.error.error("Not a 3 phase message")


def malignInstancesOfNode(node: TestNode, malignMethod, instId: int=None):
    if instId is not None:
        malignMethod(replica=node.replicas[instId])
    else:
        for r in node.replicas:
            malignMethod(replica=r)

    return node


def send3PhaseMsgWithIncorrectDigest(node: TestNode, msgType: ThreePhaseMsg,
                                     instId: int=None):
    def evilSendPrePrepareRequest(self, ppReq: PrePrepare):
        logger.debug("EVIL: Creating pre-prepare message for request : {}".
                     format(ppReq))
        ppReq = updateNamedTuple(ppReq, digest=ppReq.digest + 'random')
        self.sentPrePrepares[self.viewNo, self.lastPrePrepareSeqNo] = ppReq
        self.send(ppReq, TPCStat.PrePrepareSent)

    def evilSendPrepare(self, ppReq):
        digest = "random"
        prepare = Prepare(self.instId,
                          ppReq.viewNo,
                          ppReq.ppSeqNo,
                          ppReq.ppTime,
                          digest,
                          ppReq.stateRootHash,
                          ppReq.txnRootHash)
        logger.debug("EVIL: Creating prepare message for request {}: {}".
                     format(ppReq, prepare))
        self.addToPrepares(prepare, self.name)
        self.send(prepare, TPCStat.PrepareSent)

    def evilSendCommit(self, request):
        commit = Commit(self.instId,
                        request.viewNo,
                        request.ppSeqNo)
        logger.debug("EVIL: Creating commit message for request {}: {}".
                     format(request, commit))
        self.send(commit, TPCStat.CommitSent)
        self.addToCommits(commit, self.name)

    methodMap = {
        PrePrepare: evilSendPrePrepareRequest,
        Prepare: evilSendPrepare,
        Commit: evilSendCommit
    }

    malMethod = partial(malign3PhaseSendingMethod, msgType=msgType,
                        evilMethod=methodMap[msgType])

    malignInstancesOfNode(node, malMethod, instId)
    return node


def faultyReply(node):
    # create a variable pointing to the old method so the new method can close
    # over it and execute it
    oldGenerateReply = node.generateReply

    def newGenerateReply(self, viewNo: int, req: Request) -> Reply:
        reply = oldGenerateReply(viewNo, req)
        reply.result[f.SIG.nm] = "incorrect signature"
        reply.result["declaration"] = "All your base are belong to us."
        return reply
    node.generateReply = types.MethodType(newGenerateReply, node)


def slow_primary(nodes, inst_id=0, delay=5):
    # make primary replica slow to send PRE-PREPAREs
    def ifPrePrepare(msg):
        if isinstance(msg, PrePrepare):
            return delay

    pr = getPrimaryReplica(nodes, inst_id)
    pr.outBoxTestStasher.delay(ifPrePrepare)
    return pr


def slow_non_primary(nodes, inst_id=0, delay=5):
    # make non-primary replica slow to receive PRE-PREPAREs
    npr = getNonPrimaryReplicas(nodes, inst_id)[0]
    slow_node = npr.node
    slow_node.nodeIbStasher.delay(ppDelay(delay, inst_id))
    return npr
