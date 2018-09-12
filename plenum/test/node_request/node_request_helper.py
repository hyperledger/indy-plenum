from functools import partial

from plenum.common.messages.node_messages import PrePrepare
from plenum.common.types import OPERATION, f
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.util import getMaxFailures, get_utc_epoch
from plenum.server.node import Node
from plenum.server.quorums import Quorums
from plenum.server.replica import Replica
from plenum.test import waits
from plenum.test.bls.helper import init_discarded
from plenum.test.helper import chk_all_funcs
from plenum.test.spy_helpers import getAllArgs
from plenum.test.test_node import TestNode, getNonPrimaryReplicas, \
    getAllReplicas, getPrimaryReplica


# This code is unclear, refactor
def checkPropagated(looper, txnPoolNodeSet, request, faultyNodes=0):
    nodesSize = len(list(txnPoolNodeSet))

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

    timeout = waits.expectedPropagateTime(len(txnPoolNodeSet))
    funcs = [partial(g, node) for node in txnPoolNodeSet]
    chk_all_funcs(looper, funcs, faultyNodes, timeout)


def checkPrePrepared(looper,
                     txnPoolNodeSet,
                     propagated1,
                     instIds,
                     faultyNodes=0,
                     timeout=30):
    nodesSize = len(list(txnPoolNodeSet))

    def g(instId):
        primary = getPrimaryReplica(txnPoolNodeSet, instId)
        nonPrimaryReplicas = getNonPrimaryReplicas(txnPoolNodeSet, instId)

        def primarySeesCorrectNumberOfPREPREPAREs():
            """
            no of PRE-PREPARE as seen by processPrePrepare
            method for primary must be 0 with or without faults in system
            """
            l1 = len([param for param in
                      getAllArgs(primary, primary.processPrePrepare)])
            assert l1 == 0, 'Primary {} sees no pre-prepare'.format(primary)

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
                get_utc_epoch(),
                [propagated1.digest],
                init_discarded(),
                Replica.batchDigest([propagated1, ]),
                DOMAIN_LEDGER_ID,
                primary.stateRootHash(DOMAIN_LEDGER_ID),
                primary.txnRootHash(DOMAIN_LEDGER_ID),
                0,
                True
            )

            passes = 0
            for npr in nonPrimaryReplicas:
                actualMsgs = len([param for param in
                                  getAllArgs(npr, npr.processPrePrepare)
                                  if (param['pre_prepare'][0:3] +
                                      param['pre_prepare'][4:],
                                      param['sender']) == (
                                      expectedPrePrepareRequest[0:3] +
                                      expectedPrePrepareRequest[4:],
                                      primary.name)])

                numOfMsgsWithZFN = 1
                numOfMsgsWithFaults = 0

                passes += int(msgCountOK(nodesSize,
                                         faultyNodes,
                                         actualMsgs,
                                         numOfMsgsWithZFN,
                                         numOfMsgsWithFaults))
            assert passes >= len(nonPrimaryReplicas) - faultyNodes, \
                'Non-primary sees correct number pre-prepares - {}'.format(passes)

        def primarySentsCorrectNumberOfPREPREPAREs():
            """
            1. no of PRE-PREPARE sent by primary is 1 with or without
            fault in system but, when primary is faulty no of sent PRE_PREPARE
             will be zero and primary must be marked as malicious.
            """
            actualMsgs = len([param for param in
                              getAllArgs(primary, primary.sendPrePrepare)
                              if param['ppReq'].reqIdr[0] == propagated1.digest
                              and param['ppReq'].digest ==
                              primary.batchDigest([propagated1, ])])

            numOfMsgsWithZFN = 1

            # TODO: Considering, Primary is not faulty and will always send
            # PRE-PREPARE. Write separate test for testing when Primary
            # is faulty
            assert msgCountOK(nodesSize,
                              faultyNodes,
                              actualMsgs,
                              numOfMsgsWithZFN,
                              numOfMsgsWithZFN), 'Primary sends correct number of per-prepare'

        def nonPrimaryReceivesCorrectNumberOfPREPREPAREs():
            """
            1. no of PRE-PREPARE received by non-primaries must be 1
            with zero faults in system, and 0 faults in system.
            """
            passes = 0
            for npr in nonPrimaryReplicas:
                l4 = len([param for param in
                          getAllArgs(npr, npr.addToPrePrepares)
                          if param['pp'].reqIdr[0] == propagated1.digest
                          and param['pp'].digest ==
                          primary.batchDigest([propagated1, ])])

                numOfMsgsWithZFN = 1
                numOfMsgsWithFaults = 0

                passes += msgCountOK(nodesSize,
                                     faultyNodes,
                                     l4,
                                     numOfMsgsWithZFN,
                                     numOfMsgsWithFaults)

            assert passes >= len(nonPrimaryReplicas) - faultyNodes, \
                'Non-primary receives correct number of pre-prepare -- {}'.format(passes)

        primarySeesCorrectNumberOfPREPREPAREs()
        nonPrimarySeesCorrectNumberOfPREPREPAREs()
        primarySentsCorrectNumberOfPREPREPAREs()
        nonPrimaryReceivesCorrectNumberOfPREPREPAREs()

    funcs = [partial(g, instId) for instId in instIds]
    # TODO Select or create the timeout from 'waits'. Don't use constant.
    # looper.run(eventuallyAll(*coros, retryWait=1, totalTimeout=timeout))
    chk_all_funcs(looper, funcs, faultyNodes, timeout)


def checkPrepared(looper, txnPoolNodeSet, preprepared1, instIds, faultyNodes=0,
                  timeout=30):
    nodeCount = len(list(txnPoolNodeSet))
    quorums = Quorums(nodeCount)

    def g(instId):
        allReplicas = getAllReplicas(txnPoolNodeSet, instId)
        primary = getPrimaryReplica(txnPoolNodeSet, instId)
        nonPrimaryReplicas = getNonPrimaryReplicas(txnPoolNodeSet, instId)

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
            n-f-1 with faults.
            """
            passes = 0
            numOfMsgsWithZFN = nodeCount - 1
            numOfMsgsWithFaults = quorums.prepare.value

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
             n-f-1 with faults.
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
            numOfMsgsWithFaults = quorums.prepare.value

            assert msgCountOK(nodeCount,
                              faultyNodes,
                              actualMsgs,
                              numOfMsgsWithZFN,
                              numOfMsgsWithFaults)
            # TODO what if the primary is faulty?

        def nonPrimaryReplicasReceiveCorrectNumberOfPREPAREs():
            """
            num of PREPARE seen by Non primary replica is n - 2 without
            faults and n-f-2 with faults.
            """
            passes = 0
            numOfMsgsWithZFN = nodeCount - 2
            numOfMsgsWithFaults = quorums.prepare.value - 1

            for npr in nonPrimaryReplicas:
                actualMsgs = len(
                    [
                        param for param in getAllArgs(
                        npr,
                        npr.processPrepare) if (
                                                   param['prepare'].instId,
                                                   param['prepare'].viewNo,
                                                   param['prepare'].ppSeqNo) == (
                                                   primary.instId,
                                                   primary.viewNo,
                                                   primary.lastPrePrepareSeqNo)])

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

    funcs = [partial(g, instId) for instId in instIds]
    # TODO Select or create the timeout from 'waits'. Don't use constant.
    # looper.run(eventuallyAll(*coros, retryWait=1, totalTimeout=timeout))
    chk_all_funcs(looper, funcs, faultyNodes, timeout)


def checkCommitted(looper, txnPoolNodeSet, prepared1, instIds, faultyNodes=0):
    timeout = waits.expectedCommittedTime(len(txnPoolNodeSet))
    nodeCount = len((list(txnPoolNodeSet)))
    quorums = Quorums(nodeCount)

    def g(instId):
        allReplicas = getAllReplicas(txnPoolNodeSet, instId)
        primaryReplica = getPrimaryReplica(txnPoolNodeSet, instId)

        def replicas_gets_correct_num_of_COMMITs():
            """
            num of commit messages must be = n when zero fault;
            n = num of nodes and greater than or equal to
            n-f with faults.
            """
            passes = 0
            numOfMsgsWithZFN = quorums.commit.value
            numOfMsgsWithFault = quorums.commit.value

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

            assert passes >= min(len(allReplicas) - faultyNodes,
                                 numOfMsgsWithZFN)

        replicas_gets_correct_num_of_COMMITs()

    funcs = [partial(g, instId) for instId in instIds]
    # TODO Select or create the timeout from 'waits'. Don't use constant.
    # looper.run(eventuallyAll(*coros, retryWait=1, totalTimeout=timeout))
    chk_all_funcs(looper, funcs, faultyNodes, timeout)


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


def chk_commits_prepares_recvd(count, receivers, sender):
    counts = {}
    sender_replica_names = {r.instId: r.name for r in sender.replicas.values()}
    for node in receivers:
        for replica in node.replicas.values():
            if replica.instId not in counts:
                counts[replica.instId] = 0
            nm = sender_replica_names[replica.instId]
            for commit in replica.commits.values():
                counts[replica.instId] += int(nm in commit.voters)
            for prepare in replica.prepares.values():
                counts[replica.instId] += int(nm in prepare.voters)
    for c in counts.values():
        assert count == c, "expected {}, but have {}".format(count, c)
