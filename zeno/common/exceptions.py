from zeno.server.suspicion_codes import Suspicion


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
    def __str__(self):
        return "{}{}".format(self.__class__.__name__, self.args)


class SigningException(BaseExc):
    pass


class InvalidSignature(SigningException):
    pass


class CouldNotAuthenticate(SigningException):
    pass


class EmptySignature(SigningException):
    pass


class MissingSignature(SigningException):
    pass


class EmptyIdentifier(SigningException):
    pass


class MissingIdentifier(SigningException):
    pass


class InvalidIdentifier(SigningException):
    pass


class SuspiciousNode(Exception):
    def __init__(self, suspicion: Suspicion=None):
        self.code = suspicion.code if suspicion else None
        self.reason = suspicion.reason if suspicion else None

    def __repr__(self):
        return "Error code: {}. {}".format(self.code, self.reason)


class SuspiciousClient(BaseExc):
    pass


class InvalidMessageException(BaseExc):
    pass


class InvalidNodeMessageException(InvalidMessageException):
    pass


class InvalidClientMessageException(InvalidMessageException):
    pass


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


