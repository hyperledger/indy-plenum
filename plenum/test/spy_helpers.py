from typing import Tuple, List, Optional, Any, Union, Callable

from plenum.common.request import Request
from plenum.common.util import getCallableName
from plenum.server.node import Node
from plenum.test.testable import SpyableMethod


def getLastMsgReceivedForNode(node, method: str = None) -> Tuple:
    return node.spylog.getLast(
            method if method else Node.handleOneNodeMsg.__name__,
            required=True).params[
        'wrappedMsg']  # params should return a one element tuple


def getAllMsgReceivedForNode(node, method: str = None) -> List:
    return [m.params['msg'] for m in
            node.spylog.getAll(method if method else "eatTestMsg")]


def getLastClientReqReceivedForNode(node) -> Optional[Request]:
    requestEntry = node.spylog.getLast(Node.processRequest.__name__)
    # params should return a one element tuple
    return requestEntry.params['request'] if requestEntry else None


def getAllArgs(obj: Any, method: Union[str, Callable]) -> List[Any]:
    # params should return a List
    methodName = method if isinstance(method, str) else getCallableName(method)
    return [m.params for m in obj.spylog.getAll(methodName)]


def getAllReturnVals(obj: Any, method: SpyableMethod) -> List[Any]:
    # params should return a List
    methodName = method if isinstance(method, str) else getCallableName(method)
    return [m.result for m in obj.spylog.getAll(methodName)]


def get_count(obj: Any, method: SpyableMethod) -> List[Any]:
    return obj.spylog.count(method)
