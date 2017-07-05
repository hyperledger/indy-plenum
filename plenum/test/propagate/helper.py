from plenum.common.messages.node_messages import Propagate
from plenum.test.spy_helpers import getAllArgs
from plenum.test.test_node import TestNode


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
