from plenum.common.messages.node_messages import Propagate, PrePrepare, Prepare, Commit
from plenum.common.stashing_router import StashingRouter
from plenum.test.spy_helpers import getAllArgs
from plenum.test.test_node import TestNode
from plenum.server.replica import Replica


def sentPropagate(node: TestNode):
    params = getAllArgs(node, TestNode.send)
    return [p for p in params if isinstance(p['msg'], Propagate)]


def recvdPropagate(node: TestNode):
    return getAllArgs(node,
                      TestNode.processPropagate)


def recvdRequest(node: TestNode):
    return getAllArgs(node,
                      TestNode.processRequest)


def forwardedRequest(node: TestNode):
    return getAllArgs(node,
                      TestNode.forward)

def recvdPrePrepareForInstId(node: TestNode, instId: int):
    params = getAllArgs(node.replicas[instId].stasher, StashingRouter._process)
    return [p for p in params if isinstance(p['message'], PrePrepare)]


def recvdPrepareForInstId(node: TestNode, instId: int):
    params = getAllArgs(node.replicas[instId].stasher, StashingRouter._process)
    return [p for p in params if isinstance(p['message'], Prepare)]


def recvdCommitForInstId(node: TestNode, instId: int):
    params = getAllArgs(node.replicas[instId].stasher, StashingRouter._process)
    return [p for p in params if isinstance(p['message'], Commit)]
