from plenum.server.suspicion_codes import Suspicion
from re import compile, match


class NodeError(Exception):
    pass


class PortNotAvailableForNodeWebServer(NodeError):
    pass


class RemoteError(NodeError):
    def __init__(self, remote):
        self.remote = remote


class RemoteNotFound(RemoteError):
    pass


class BaseExc(Exception):
    # def __init__(self, code: int=None, reason: str=None):
    #     self.code = code
    #     self.reason = reason
    def __str__(self):
        return "{}{}".format(self.__class__.__name__, self.args)


class SigningException(BaseExc):
    pass


class CouldNotAuthenticate(SigningException):
    code = 110
    reason = 'could not authenticate'


class MissingSignature(SigningException):
    code = 120
    reason = 'missing signature'


class EmptySignature(SigningException):
    code = 121
    reason = 'empty signature'

    def __init__(self, identifier, reqId):
        self.identifier = identifier
        self.reqId = reqId


class InvalidSignature(SigningException):
    code = 125
    reason = 'invalid signature'


class MissingIdentifier(SigningException):
    code = 130
    reason = 'missing identifier'


class EmptyIdentifier(SigningException):
    code = 131
    reason = 'empty identifier'


class InvalidIdentifier(SigningException):
    code = 135
    reason = 'invalid identifier'

    def __init__(self, identifier, reqId):
        self.identifier = identifier
        self.reqId = reqId


class SuspiciousNode(BaseExc):
    def __init__(self, node: str, suspicion: Suspicion, offendingMsg):
        self.code = suspicion.code if suspicion else None
        self.reason = suspicion.reason if suspicion else None
        p = compile(r'(\b\w+)(:(\d+))?')
        m = p.match(node)
        self.node = m.groups()[0] if m else node
        self.offendingMsg = offendingMsg

    def __repr__(self):
        return "Error code: {}. {}".format(self.code, self.reason)


class SuspiciousClient(BaseExc):
    pass


class InvalidMessageException(BaseExc):
    pass


class InvalidNodeMessageException(InvalidMessageException):
    pass


class InvalidClientMessageException(InvalidMessageException):
    def __init__(self, identifier, reqId, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.identifier = identifier
        self.reqId = reqId


class InvalidNodeMsg(InvalidNodeMessageException):
    pass


class MissingNodeOp(InvalidNodeMsg):
    pass


class InvalidNodeOp(InvalidNodeMsg):
    pass


class InvalidNodeMsgType(InvalidNodeMsg):
    pass


class InvalidClientRequest(InvalidClientMessageException):
    pass


class InvalidClientMsgType(InvalidClientRequest):
    pass


class InvalidClientOp(InvalidClientRequest):
    pass


class UnauthorizedClientRequest(InvalidClientMessageException):
    pass
