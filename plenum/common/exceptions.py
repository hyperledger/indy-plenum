from plenum.server.suspicion_codes import Suspicion
from re import compile, match


class ReqInfo:
    def __init__(self, identifier=None, reqId=None):
        self.identifier = identifier
        self.reqId = reqId


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


class CouldNotAuthenticate(SigningException, ReqInfo):
    code = 110
    reason = 'could not authenticate'

    def __init__(self, *args, **kwargs):
        ReqInfo.__init__(self, *args, **kwargs)


class MissingSignature(SigningException):
    code = 120
    reason = 'missing signature'


class EmptySignature(SigningException, ReqInfo):
    code = 121
    reason = 'empty signature'

    def __init__(self, *args, **kwargs):
        ReqInfo.__init__(self, *args, **kwargs)


class InvalidSignatureFormat(SigningException, ReqInfo):
    code = 123
    reason = 'invalid signature format'

    def __init__(self, *args, **kwargs):
        ReqInfo.__init__(self, *args, **kwargs)


class InvalidSignature(SigningException, ReqInfo):
    code = 125
    reason = 'invalid signature'

    def __init__(self, *args, **kwargs):
        ReqInfo.__init__(self, *args, **kwargs)


class MissingIdentifier(SigningException):
    code = 130
    reason = 'missing identifier'


class EmptyIdentifier(SigningException):
    code = 131
    reason = 'empty identifier'


class UnknownIdentifier(SigningException, ReqInfo):
    code = 133
    reason = 'unknown identifier'

    def __init__(self, *args, **kwargs):
        ReqInfo.__init__(self, *args, **kwargs)


class InvalidIdentifier(SigningException, ReqInfo):
    code = 135
    reason = 'invalid identifier'

    def __init__(self, *args, **kwargs):
        ReqInfo.__init__(self, *args, **kwargs)


class UnregisteredIdentifier(SigningException):
    code = 136
    reason = 'provided owner identifier not registered with agent'


class RaetKeysNotFoundException(Exception):
    code = 141
    reason = 'Keys not found in the keep for {}. ' \
             'To generate them run script '


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


class SuspiciousClient(BaseExc, ReqInfo):
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
        self.args = args


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


class StorageException(Exception):
    pass


class DataDirectoryNotFound(StorageException):
    pass


class DBConfigNotFound(StorageException):
    pass


class UnsupportedOperation(Exception):
    pass


class DidMethodNotFound(Exception):
    pass


class BlowUp(BaseException):
    """
    An exception designed to blow through fault barriers. Useful during testing.
    Derives from BaseException so asyncio will let it through.
    """
    pass


class ProdableAlreadyAdded(Exception):
    pass


class NoConsensusYet(Exception):
    pass


class NotConnectedToAny(Exception):
    pass


class NameAlreadyExists(Exception):
    pass


class GraphStorageNotAvailable(Exception):
    pass


class OrientDBNotRunning(GraphStorageNotAvailable):
    pass


class EndpointException(Exception):
    pass


class MissingEndpoint(EndpointException):
    def __init__(self):
        super().__init__('missing endpoint')


class InvalidEndpointIpAddress(EndpointException):
    def __init__(self, endpoint):
        super().__init__("invalid endpoint address: '{}'".format(endpoint))


class InvalidEndpointPort(EndpointException):
    def __init__(self, endpoint):
        super().__init__("invalid endpoint port: '{}'".format(endpoint))


class PortNotAvailable(Exception):
    def __init__(self, port):
        self.port = port
        super().__init__("port not available: {}".format(port))


class OperationError(Exception):
    def __init__(self, error):
        super().__init__("error occurred during operation: {}".format(error))
