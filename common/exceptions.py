# -*- coding: utf-8 -*-
from typing import Any


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
