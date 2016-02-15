import random
import types
from functools import partial

from zeno.common.request_types import Propagate, Request, \
    PrePrepare, Prepare, ReqDigest, ThreePhaseMsg, Commit, Reply

from zeno.common import util
from zeno.common.util import getlogger, updateNamedTuple
from zeno.test.helper import TestNode, TestReplica
from zeno.test.helper import ppDelay

logger = getlogger()


def makeNodeFaulty(node, *behaviors):
    for behavior in behaviors:
        behavior(node)


def changesRequest(node):
    def evilCreatePropagate(self,
                            request: Request) -> Propagate:
        logger.debug("EVIL: Creating propagate request for client request {}".
                     format(request))
        request.operation["amount"] += random.random()
        return Propagate(request.__getstate__())

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
        prePrepare = PrePrepare(self.instId, self.viewNo,
                                self.prePrepareSeqNo, *reqDigest)
        self.sentPrePrepares[self.viewNo, self.prePrepareSeqNo] = reqDigest
        sendDup(self, prePrepare, count)

    def evilSendPrepare(self, request):
        logger.debug("EVIL: Creating prepare message for request {}".
                     format(request))
        prepare = Prepare(self.instId,
                          request.viewNo,
                          request.ppSeqNo,
                          request.digest)
        self.addToPrepares(prepare, self.name)
        sendDup(self, prepare, count)

    def evilSendCommit(self, request):
        logger.debug("EVIL: Creating commit message for request {}".
                     format(request))
        commit = Commit(self.instId,
                        request.viewNo,
                        request.ppSeqNo,
                        request.digest)
        self.addToCommits(commit, self.name)
        sendDup(self, commit, count)

    def sendDup(sender, msg, count: int):
        for i in range(count):
            sender.send(msg)

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
        replica.sendPrepare = evilMethod
    elif msgType == Commit:
        replica.sendCommit = evilMethod
    else:
        util.error("Not a 3 phase message")


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
        reqDigest = ReqDigest(reqDigest.clientId, reqDigest.reqId, "random")
        prePrepare = PrePrepare(self.instId, self.viewNo,
                                self.prePrepareSeqNo, *reqDigest)
        self.sentPrePrepares[self.viewNo, self.prePrepareSeqNo] = reqDigest
        self.send(prePrepare)

    def evilSendPrepare(self, request):
        logger.debug("EVIL: Creating prepare message for request {}".
                     format(request))
        digest = "random"
        prepare = Prepare(self.instId,
                          request.viewNo,
                          request.ppSeqNo,
                          digest)
        self.addToPrepares(prepare, self.name)
        self.send(prepare)

    def evilSendCommit(self, request):
        logger.debug("EVIL: Creating commit message for request {}".
                     format(request))
        digest = "random"
        commit = Commit(self.instId,
                        request.viewNo,
                        request.ppSeqNo,
                        digest)
        self.addToCommits(commit, self.name)
        self.send(commit)

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
