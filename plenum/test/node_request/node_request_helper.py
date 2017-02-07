import time
from functools import partial

from plenum.common.eventually import eventuallyAll
from plenum.common.types import PrePrepare, OPERATION, f, DOMAIN_LEDGER_ID
from plenum.common.util import getMaxFailures
from plenum.server.node import Node
from plenum.server.replica import Replica
from plenum.test.helper import getPrimaryReplica
from plenum.test.spy_helpers import getAllArgs
from plenum.test.test_node import TestNode, getNonPrimaryReplicas, \
    getAllReplicas


def checkPropagated(looper, nodeSet, request, faultyNodes=0):
    nodesSize = len(list(nodeSet.nodes))

    # noinspection PyIncorrectDocstring
    def g(node: TestNode):
        """
        1. no of propagate received by node must be n -1 with zero
            faulty nodes in system; where n = num of nodes
        2. no of propagate received by node must be greater than
         or equal to f + 1
        """
        actualMsgs = len([x for x in
                          getAllArgs(node, Node.processPropagate)
                          if x['msg'].request[f.REQ_ID.nm] == request.reqId and
                          x['msg'].request[f.IDENTIFIER.nm] == request.identifier and
                          x['msg'].request[OPERATION] == request.operation])

        numOfMsgsWithZFN = nodesSize - 1
        numOfMsgsWithFaults = faultyNodes + 1

        assert msgCountOK(nodesSize,
                          faultyNodes,
                          actualMsgs,
                          numOfMsgsWithZFN,
                          numOfMsgsWithFaults)

    coros = [partial(g, node) for node in nodeSet]
    looper.run(eventuallyAll(*coros,
                             totalTimeout=10,
                             acceptableFails=faultyNodes))


def checkPrePrepared(looper,
                     nodeSet,
                     propagated1,
                     instIds,
                     faultyNodes=0):
    nodesSize = len(list(nodeSet))

    def g(instId):
        primary = getPrimaryReplica(nodeSet, instId)
        nonPrimaryReplicas = getNonPrimaryReplicas(nodeSet, instId)

        def primarySeesCorrectNumberOfPREPREPAREs():
            """
            no of PRE-PREPARE as seen by processPrePrepare
            method for primary must be 0 with or without faults in system
            """
            l1 = len([param for param in
                      getAllArgs(primary, primary.processPrePrepare)])
            assert l1 == 0

        def nonPrimarySeesCorrectNumberOfPREPREPAREs():
            """
            1. no of PRE-PREPARE as seen by processPrePrepare method for
            non-primaries must be 1; whn zero faulty nodes in system.

            2. no of PRE-PREPARE as seen by processPrePrepare method for
            non-primaries must be greater than or equal to 0;
            with faults in system.
            """
            expectedPrePrepareRequest = PrePrepare(
                    instId,
                    primary.viewNo,
                    primary.lastPrePrepareSeqNo,
                    time.time(),
                    [[propagated1.identifier, propagated1.reqId]],
                    1,
                    Replica.batchDigest([propagated1,]),
                    DOMAIN_LEDGER_ID,
                    primary.stateRoot(DOMAIN_LEDGER_ID),
                    primary.txnRoot(DOMAIN_LEDGER_ID),
                    )

            passes = 0
            for npr in nonPrimaryReplicas:
                actualMsgs = len([param for param in
                                  getAllArgs(npr, npr.processPrePrepare)
                                  if (param['pp'][0:3]+param['pp'][4:],
                                      param['sender']) == (
                                      expectedPrePrepareRequest[0:3] + expectedPrePrepareRequest[4:],
                                      primary.name)])

                numOfMsgsWithZFN = 1
                numOfMsgsWithFaults = 0

                passes += int(msgCountOK(nodesSize,
                                         faultyNodes,
                                         actualMsgs,
                                         numOfMsgsWithZFN,
                                         numOfMsgsWithFaults))
            assert passes >= len(nonPrimaryReplicas) - faultyNodes

        def primarySentsCorrectNumberOfPREPREPAREs():
            """
            1. no of PRE-PREPARE sent by primary is 1 with or without
            fault in system but, when primary is faulty no of sent PRE_PREPARE
             will be zero and primary must be marked as malicious.
            """
            actualMsgs = len([param for param in
                              getAllArgs(primary, primary.sendPrePrepare)
                              if (param['ppReq'].reqIdr[0][0],
                                  param['ppReq'].reqIdr[0][1],
                                  param['ppReq'].digest) ==
                              (propagated1.identifier,
                               propagated1.reqId,
                               primary.batchDigest([propagated1, ]))
                              ])

            numOfMsgsWithZFN = 1

            # TODO: Considering, Primary is not faulty and will always send
            # PRE-PREPARE. Write separate test for testing when Primary
            # is faulty
            assert msgCountOK(nodesSize,
                              faultyNodes,
                              actualMsgs,
                              numOfMsgsWithZFN,
                              numOfMsgsWithZFN)

        def nonPrimaryReceivesCorrectNumberOfPREPREPAREs():
            """
            1. no of PRE-PREPARE received by non-primaries must be 1
            with zero faults in system, and 0 faults in system.
            """
            passes = 0
            for npr in nonPrimaryReplicas:
                l4 = len([param for param in
                          getAllArgs(npr, npr.addToPrePrepares)
                          if (param['pp'].reqIdr[0][0],
                              param['pp'].reqIdr[0][1],
                              param['pp'].digest) == (
                              propagated1.identifier,
                              propagated1.reqId,
                              primary.batchDigest([propagated1, ]))])

                numOfMsgsWithZFN = 1
                numOfMsgsWithFaults = 0

                passes += msgCountOK(nodesSize,
                                     faultyNodes,
                                     l4,
                                     numOfMsgsWithZFN,
                                     numOfMsgsWithFaults)

            assert passes >= len(nonPrimaryReplicas) - faultyNodes

        primarySeesCorrectNumberOfPREPREPAREs()
        nonPrimarySeesCorrectNumberOfPREPREPAREs()
        primarySentsCorrectNumberOfPREPREPAREs()
        nonPrimaryReceivesCorrectNumberOfPREPREPAREs()

    coros = [partial(g, instId) for instId in instIds]
    looper.run(eventuallyAll(*coros, retryWait=1, totalTimeout=30))


def checkPrepared(looper, nodeSet, preprepared1, instIds, faultyNodes=0):
    nodeCount = len(list(nodeSet.nodes))
    f = getMaxFailures(nodeCount)

    def g(instId):
        allReplicas = getAllReplicas(nodeSet, instId)
        primary = getPrimaryReplica(nodeSet, instId)
        nonPrimaryReplicas = getNonPrimaryReplicas(nodeSet, instId)

        def primaryDontSendAnyPREPAREs():
            """
            1. no of PREPARE sent by primary should be 0
            """
            for r in allReplicas:
                for param in getAllArgs(r, Replica.processPrepare):
                    sender = param['sender']
                    assert sender != primary.name

        def allReplicasSeeCorrectNumberOfPREPAREs():
            """
            1. no of PREPARE received by replicas must be n - 1;
            n = num of nodes without fault, and greater than or equal to
             2f with faults.
            """
            passes = 0
            numOfMsgsWithZFN = nodeCount - 1
            numOfMsgsWithFaults = 2 * f

            for replica in allReplicas:
                key = primary.viewNo, primary.lastPrePrepareSeqNo
                if key in replica.prepares:
                    actualMsgs = len(replica.prepares[key].voters)

                    passes += int(msgCountOK(nodeCount,
                                             faultyNodes,
                                             actualMsgs,
                                             numOfMsgsWithZFN,
                                             numOfMsgsWithFaults))
            assert passes >= len(allReplicas) - faultyNodes

        def primaryReceivesCorrectNumberOfPREPAREs():
            """
            num of PREPARE seen by primary replica is n - 1;
                n = num of nodes without fault, and greater than or equal to
             2f with faults.
            """
            actualMsgs = len([param for param in
                              getAllArgs(primary,
                                         primary.processPrepare)
                              if (param['prepare'].instId,
                                  param['prepare'].viewNo,
                                  param['prepare'].ppSeqNo) == (
                                  primary.instId, primary.viewNo,
                                  primary.lastPrePrepareSeqNo) and
                              param['sender'] != primary.name])

            numOfMsgsWithZFN = nodeCount - 1
            numOfMsgsWithFaults = 2 * f - 1

            assert msgCountOK(nodeCount,
                              faultyNodes,
                              actualMsgs,
                              numOfMsgsWithZFN,
                              numOfMsgsWithFaults)
            # TODO what if the primary is faulty?

        def nonPrimaryReplicasReceiveCorrectNumberOfPREPAREs():
            """
            num of PREPARE seen by Non primary replica is n - 2 without
            faults and 2f - 1 with faults.
            """
            passes = 0
            numOfMsgsWithZFN = nodeCount - 2
            numOfMsgsWithFaults = (2 * f) - 1

            for npr in nonPrimaryReplicas:
                actualMsgs = len([param for param in
                                  getAllArgs(
                                          npr,
                                          npr.processPrepare)
                                  if (param['prepare'].instId,
                                      param['prepare'].viewNo,
                                      param['prepare'].ppSeqNo) == (primary.instId,
                                                            primary.viewNo,
                                                            primary.lastPrePrepareSeqNo)
                                  ])

                passes += int(msgCountOK(nodeCount,
                                         faultyNodes,
                                         actualMsgs,
                                         numOfMsgsWithZFN,
                                         numOfMsgsWithFaults))

            assert passes >= len(nonPrimaryReplicas) - faultyNodes
            # TODO how do we know if one of the faulty nodes is a primary or
            # not?

        primaryDontSendAnyPREPAREs()
        allReplicasSeeCorrectNumberOfPREPAREs()
        primaryReceivesCorrectNumberOfPREPAREs()
        nonPrimaryReplicasReceiveCorrectNumberOfPREPAREs()

    coros = [partial(g, instId) for instId in instIds]
    looper.run(eventuallyAll(*coros, retryWait=1, totalTimeout=30))


def checkCommited(looper, nodeSet, prepared1, instIds, faultyNodes=0):
    nodeCount = len((list(nodeSet)))
    f = getMaxFailures(nodeCount)

    def g(instId):
        allReplicas = getAllReplicas(nodeSet, instId)
        primaryReplica = getPrimaryReplica(nodeSet, instId)

        def replicasSeesCorrectNumOfCOMMITs():
            """
            num of commit messages must be = n when zero fault;
            n = num of nodes and greater than or equal to
            2f + 1 with faults.
            """
            passes = 0
            numOfMsgsWithZFN = nodeCount
            numOfMsgsWithFault = (2 * f) + 1

            key = (primaryReplica.viewNo, primaryReplica.lastPrePrepareSeqNo)
            for r in allReplicas:
                if key in r.commits:
                    rcvdCommitRqst = r.commits[key]
                    actualMsgsReceived = len(rcvdCommitRqst.voters)

                    passes += int(msgCountOK(nodeCount,
                                             faultyNodes,
                                             actualMsgsReceived,
                                             numOfMsgsWithZFN,
                                             numOfMsgsWithFault))

            assert passes >= len(allReplicas) - faultyNodes

        def replicasReceivesCorrectNumberOfCOMMITs():
            """
            num of commit messages seen by replica must be equal to n - 1;
            when zero fault and greater than or equal to
            2f+1 with faults.
            """
            passes = 0
            numOfMsgsWithZFN = nodeCount - 1
            numOfMsgsWithFault = 2 * f

            for r in allReplicas:
                args = getAllArgs(r, r.processCommit)
                actualMsgsReceived = len(args)

                passes += int(msgCountOK(nodeCount,
                                         faultyNodes,
                                         actualMsgsReceived,
                                         numOfMsgsWithZFN,
                                         numOfMsgsWithFault))

                for arg in args:
                    assert arg['commit'].viewNo == primaryReplica.viewNo and \
                           arg['commit'].ppSeqNo == primaryReplica.lastPrePrepareSeqNo
                    assert r.name != arg['sender']

            assert passes >= len(allReplicas) - faultyNodes

        replicasReceivesCorrectNumberOfCOMMITs()
        replicasSeesCorrectNumOfCOMMITs()

    coros = [partial(g, instId) for instId in instIds]
    looper.run(eventuallyAll(*coros, retryWait=1, totalTimeout=60))


def msgCountOK(nodesSize,
               faultyNodes,
               actualMessagesReceived,
               numOfMsgsWithZNF,
               numOfSufficientMsgs):
    if faultyNodes == 0:
        return actualMessagesReceived == numOfMsgsWithZNF

    elif faultyNodes <= getMaxFailures(nodesSize):
        return actualMessagesReceived >= numOfSufficientMsgs
    else:
        # Less than or equal to `numOfSufficientMsgs` since the faults may
        # not reduce the number of correct messages
        return actualMessagesReceived <= numOfSufficientMsgs
