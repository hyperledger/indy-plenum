# -*- coding: utf-8 -*-
from typing import Any


class PlenumError(Exception):
    """Base exceptions class for Plenum exceptions"""

    @staticmethod
    def _prefix_msg(msg, prefix=None):
        return "{}{}".format(
            "" if prefix is None else "{}: ".format(prefix),
            msg
        )


class PlenumTypeError(PlenumError, TypeError):
    """Wrapper exception for TypeError

    Extends TypeError to provide formatted error message

    :param v_name: variable name
    :param v_value: variable value
    :param v_exp_t: expected variable type
    """
    def __init__(self, v_name: str, v_value: Any, v_exp_t: Any, *args,
                 prefix=None):
        super().__init__(
            self._prefix_msg(
                ("variable '{}', type {}, expected: {}"
                 .format(v_name, type(v_value), v_exp_t)),
                prefix),
            *args)


class PlenumValueError(PlenumError, ValueError):
    """Wrapper exception for ValueError

    Extends ValueError to provide formatted error message

    :param v_name: variable name
    :param v_value: variable value
    :param v_exp_value: expected variable value
    :param prefix: (optional) prefix for the message
    """
    def __init__(self, v_name: str, v_value: Any, v_exp_value: Any, *args,
                 prefix=None):
        super().__init__(
            self._prefix_msg(
                ("variable '{}', value {}, expected: {}"
                 .format(v_name, v_value, v_exp_value)),
                prefix),
            *args)


class ValueUndefinedError(PlenumError, ValueError):
    """Wrapper exception for ValueError

    Extends ValueError to provide formatted error message

    :param v_name: variable name
    """
    def __init__(self, v_name: str, *args, prefix=None):
        super().__init__(
            self._prefix_msg(
                "variable '{}' is undefined".format(v_name),
                prefix),
            *args)


class LogicError(PlenumError, RuntimeError):
    """Some logic assumption is wrong"""
    pass
