from plenum.common.request_types import Propagate
from plenum.test.helper import TestNode, getAllArgs


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
