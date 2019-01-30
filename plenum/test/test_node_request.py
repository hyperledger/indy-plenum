import pytest

from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from stp_core.loop.looper import Looper
from plenum.common.messages.node_messages import \
    PrePrepare, Prepare, Commit
from plenum.test import waits
from plenum.test.greek import genNodeNames
from plenum.test.helper import assertLength, addNodeBack, \
    sdk_send_random_and_check
from plenum.test.test_node import TestNode, TestNodeSet, \
    checkPoolReady, genNodeReg, prepareNodeSet

whitelist = ['cannot process incoming PREPARE']
logger = getlogger()


def testReqExecWhenReturnedByMaster(looper, txnPoolNodeSet,
                                    sdk_pool_handle,
                                    sdk_wallet_client):
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              1)

    async def chk():
        for node in txnPoolNodeSet:
            entries = node.spylog.getAll(
                node.processOrdered.__name__)
            for entry in entries:
                arg = entry.params['ordered']
                result = entry.result
                if arg.instId == node.instances.masterId:
                    assert result
                else:
                    assert result is False

    timeout = waits.expectedOrderingTime(
        txnPoolNodeSet[0].instances.count)
    looper.run(eventually(chk, timeout=timeout))


async def checkIfPropagateRecvdFromNode(recvrNode: TestNode,
                                        senderNode: TestNode, key: str):
    assert key in recvrNode.requests
    assert senderNode.name in recvrNode.requests[key].propagates


def snapshotStats(*nodes):
    return {n.name: n.nodestack.stats.copy() for n in nodes}


def statsDiff(a, b):
    diff = {}
    nodes = set(a.keys()).union(b.keys())
    for n in nodes:
        nas = a.get(n, {})
        nbs = b.get(n, {})

        keys = set(nas.keys()).union(nbs.keys())
        diff[n] = {}
        for k in keys:
            diff[n][k] = nas.get(k, 0) - nbs.get(k, 0)
    return diff
