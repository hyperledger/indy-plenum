# -*- coding: utf-8 -*-
from typing import Any, List


class PlenumError(Exception):
    """Base exceptions class for Plenum exceptions

    :param prefix: (optional) prefix for the message
    """
    def __init__(self, *args, prefix=None):
        if len(args) and prefix is not None:
            args = ("{}: {}".format(prefix, args[0]),) + args[1:]
        super().__init__(*args)


class PlenumTypeError(PlenumError, TypeError):
    """Wrapper exception for TypeError

    Extends TypeError to provide formatted error message

    :param v_name: variable name
    :param v_value: variable value
    :param v_exp_t: expected variable type
    """
    def __init__(self, v_name: str, v_value: Any, v_exp_t: Any,
                 *args, **kwargs):
        super().__init__(
            ("variable '{}', type {}, expected: {}"
             .format(v_name, type(v_value), v_exp_t)),
            *args, **kwargs
        )


class PlenumValueError(PlenumError, ValueError):
    """Wrapper exception for ValueError

    Extends ValueError to provide formatted error message

    :param v_name: variable name
    :param v_value: variable value
    :param v_exp_value: expected variable value
    """
    def __init__(self, v_name: str, v_value: Any, v_exp_value: Any, *args, **kwargs):
        super().__init__(
            ("variable '{}', value {}, expected: {}"
             .format(v_name, v_value, v_exp_value)),
            *args, **kwargs
        )


class ValueUndefinedError(PlenumError, ValueError):
    """Wrapper exception for ValueError

    Extends ValueError to provide formatted error message

    :param v_name: variable name
    """
    def __init__(self, v_name: str, *args, **kwargs):
        super().__init__(
            "variable '{}' is undefined".format(v_name),
            *args, **kwargs
        )


class LogicError(PlenumError, RuntimeError):
    """Some logic assumption is wrong"""
    pass


####################
# TRANSPORT ERRORS #
####################

class PlenumTransportError(PlenumError):
    """Base exception for errors related to transport layer.

    Identities types:
        - remote - identity with keys
        - peer - identity without keys

    :param msg (optional): message to process by transport level. Default: None.
    :param ident (optional): identity. Default: None.
    """
    def __init__(self, *args, msg=None, ident=None, **kwargs):
        self.msg = msg
        self.ident = ident
        super().__init__(*args, **kwargs)


class PlenumMultiIdentError(PlenumTransportError):
    """Cumulative error for communication with multiple identities.

    :param errros: list of errors.
    """
    def __init__(self, errors: List[PlenumTransportError], *args, **kwargs):
        self.errors = errors
        kwargs.pop('msg', None)
        kwargs.pop('ident', None)
        # TODO better presentation of errors
        super().__init__("\n".join([str(e) for e in errors]), *args, **kwargs)


class TooBigMessage(PlenumTransportError):
    """Message size is too big.

    :param msg: message.
    """
    def __init__(self, msg, *args,
                 msg_len=None, max_len=None, max_dump_len=1000, **kwargs):
        self.msg_len = msg_len
        self.max_len = max_len
        self.max_dump_len = max_dump_len
        super().__init__(
            ("Message is too big: msg_len {}, max_len {} msg {!s:.{dlen}}"
             .format(msg_len, max_len, msg, dlen=max_dump_len)),
            *args, msg=msg, **kwargs
        )


class IdentityIsUnknown(PlenumTransportError):
    """Identity is unknown.

    :param ident: identity.
    """
    def __init__(self, ident, *args, **kwargs):
        super().__init__(
            "Identity {} is unknown".format(ident),
            *args, ident=ident, **kwargs
        )


class NoSocketForIdentity(PlenumTransportError):
    """No socket for identity.

    :param ident: identity.
    """
    def __init__(self, ident, *args, **kwargs):
        super().__init__(
            "Socket for identity {} is not initialized".format(ident),
            *args, ident=ident, **kwargs
        )


class EAgainError(PlenumTransportError):
    """Wraps EAgain errors possible in non-blocking mode.

    :param msg: message.
    :param ident: identity.
    """
    def __init__(self, msg, ident, *args, **kwargs):
        super().__init__(
            ("The message cannot be sent at the moment: identity {}, msg {}."
             .format(ident, msg)),
            *args, msg=msg, ident=ident, **kwargs
        )
