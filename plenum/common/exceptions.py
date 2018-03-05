from re import compile

from plenum.server.suspicion_codes import Suspicion


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


class InsufficientSignatures(SigningException, ReqInfo):
    code = 126
    reason = 'insufficient signatures, {} provided but {} required'

    def __init__(self, provided, required, *args, **kwargs):
        self.reason = self.reason.format(provided, required)
        ReqInfo.__init__(self, *args, **kwargs)


class InsufficientCorrectSignatures(SigningException, ReqInfo):
    code = 127
    reason = 'insufficient correct signatures, {} correct but {} required'

    def __init__(self, valid, required, *args, **kwargs):
        self.reason = self.reason.format(valid, required)
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


class NoAuthenticatorFound(SigningException):
    code = 137


class KeysNotFoundException(Exception):
    code = 141
    reason = 'Keys not found in the keep for {}. ' \
             'To generate them run script '


class InvalidKey(Exception):
    code = 142
    reason = 'invalid key'


class SuspiciousNode(BaseExc):
    def __init__(self, node: str, suspicion: Suspicion, offendingMsg, addInfo: str=None):
        node = node.decode() if isinstance(node, bytes) else node
        self.code = suspicion.code if suspicion else None
        self._reason = suspicion.reason if suspicion else None
        p = compile(r'(\b\w+)(:(\d+))?')
        m = p.match(node)
        self.node = m.groups()[0] if m else node
        self.offendingMsg = offendingMsg
        self.addInfo = addInfo

    @property
    def reason(self):
        return ("{}".format(self._reason) +
                (", {}".format(self.addInfo) if self.addInfo else ""))

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


class KeyValueStorageConfigNotFound(StorageException):
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


class ProdableAlreadyAdded(Exception):
    pass


class NoConsensusYet(Exception):
    pass


class NotConnectedToAny(Exception):
    pass


class NameAlreadyExists(Exception):
    pass


class WalletError(Exception):
    pass


class WalletNotSet(WalletError):
    pass


class WalletNotInitialized(WalletError):
    pass


class PortNotAvailable(OSError):
    def __init__(self, port):
        self.port = port
        super().__init__("port not available: {}".format(port))


class OperationError(Exception):
    def __init__(self, error):
        super().__init__("error occurred during operation: {}".format(error))


class InvalidMessageExceedingSizeException(InvalidMessageException):
    def __init__(self, expLen, actLen, *args, **kwargs):
        ex_txt = 'Message len {} exceeded allowed limit of {}'.format(
            actLen, expLen)
        super().__init__(ex_txt, *args, **kwargs)
