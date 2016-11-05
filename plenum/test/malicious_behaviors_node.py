import random
import types
from functools import partial

import time

import plenum.common.error
from plenum.common.types import Propagate, PrePrepare, Prepare, ThreePhaseMsg, \
    Commit, Reply
from plenum.common.request import Request, ReqDigest

from plenum.common import util
from plenum.common.util import updateNamedTuple
from plenum.common.log import getlogger
from plenum.server.replica import TPCStat
from plenum.test.helper import TestReplica
from plenum.test.test_node import TestNode, TestReplica
from plenum.test.delayers import ppDelay

logger = getlogger()


def makeNodeFaulty(node, *behaviors):
    for behavior in behaviors:
        behavior(node)


def changesRequest(node):
    def evilCreatePropagate(self,
                            request: Request, clientName: str) -> Propagate:
        logger.debug("EVIL: Creating propagate request for client request {}".
                     format(request))
        request.operation["amount"] += random.random()
        return Propagate(request.__getstate__(), clientName)

    evilMethod = types.MethodType(evilCreatePropagate, node)
    node.createPropagate = evilMethod
    return node


def delaysPrePrepareProcessing(node, delay: float=30, instId: int=None):
    node.nodeIbStasher.delay(ppDelay(delay=delay, instId=instId))


# Could have this method directly take a replica rather than a node and an
# instance id but this looks more useful as a complete node can be malicious
def sendDuplicate3PhaseMsg(node: TestNode, msgType: ThreePhaseMsg, count: int=2,
                           instId=None):
    def evilSendPrePrepareRequest(self, reqDigest: ReqDigest):
        logger.debug("EVIL: Creating pre-prepare message for request {}".
                     format(reqDigest))
        tm = time.time()
        prePrepare = PrePrepare(self.instId, self.viewNo,
                                self.prePrepareSeqNo, *reqDigest, tm)
        self.sentPrePrepares[self.viewNo, self.prePrepareSeqNo] = (reqDigest, tm)
        sendDup(self, prePrepare, TPCStat.PrePrepareSent, count)

    def evilSendPrepare(self, request):
        logger.debug("EVIL: Creating prepare message for request {}".
                     format(request))
        prepare = Prepare(self.instId,
                          request.viewNo,
                          request.ppSeqNo,
                          request.digest,
                          request.ppTime)
        self.addToPrepares(prepare, self.name)
        sendDup(self, prepare, TPCStat.PrepareSent, count)

    def evilSendCommit(self, request):
        logger.debug("EVIL: Creating commit message for request {}".
                     format(request))
        commit = Commit(self.instId,
                        request.viewNo,
                        request.ppSeqNo,
                        request.digest,
                        request.ppTime)
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
        replica.doPrePrepare = evilMethod
    elif msgType == Prepare:
        replica.doPrepare = evilMethod
    elif msgType == Commit:
        replica.doCommit = evilMethod
    else:
        plenum.common.error.error("Not a 3 phase message")


def malignInstancesOfNode(node: TestNode, malignMethod, instId: int=None):
    if instId is not None:
        malignMethod(replica=node.replicas[instId])
    else:
        for r in node.replicas:
            malignMethod(replica=r)

    return node


def send3PhaseMsgWithIncorrectDigest(node: TestNode, msgType: ThreePhaseMsg,
                                     instId: int=None):
    def evilSendPrePrepareRequest(self, reqDigest: ReqDigest):
        logger.debug("EVIL: Creating pre-prepare message for request {}".
                     format(reqDigest))
        reqDigest = ReqDigest(reqDigest.identifier, reqDigest.reqId, "random")
        tm = time.time()
        prePrepare = PrePrepare(self.instId, self.viewNo,
                                self.prePrepareSeqNo, *reqDigest, tm)
        self.sentPrePrepares[self.viewNo, self.prePrepareSeqNo] = (reqDigest, tm)
        self.send(prePrepare, TPCStat.PrePrepareSent)

    def evilSendPrepare(self, request):
        logger.debug("EVIL: Creating prepare message for request {}".
                     format(request))
        digest = "random"
        prepare = Prepare(self.instId,
                          request.viewNo,
                          request.ppSeqNo,
                          digest,
                          request.ppTime)
        self.addToPrepares(prepare, self.name)
        self.send(prepare, TPCStat.PrepareSent)

    def evilSendCommit(self, request):
        logger.debug("EVIL: Creating commit message for request {}".
                     format(request))
        digest = "random"
        commit = Commit(self.instId,
                        request.viewNo,
                        request.ppSeqNo,
                        digest,
                        request.ppTime)
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
        reply.result["txnId"] = "For great justice."
        reply.result["declaration"] = "All your base are belong to us."
        return reply
    node.generateReply = types.MethodType(newGenerateReply, node)
