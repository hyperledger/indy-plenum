# -*- coding: utf-8 -*-
from typing import Any

class PlenumError(Exception):
    """Base exceptions class for Plenum exceptions"""
    pass


class PlenumTypeError(PlenumError, TypeError):
    """Wrapper exception for TypeError

    Extends TypeError to provide formatted error message

    :param v_name: variable name
    :param v_value: variable value
    :param v_exp_t: expected variable type
    """
    def __init__(self, v_name: str, v_value: Any, v_exp_t: Any, *args):
        super().__init__(
            ("incorrect type of '{}' ({}), should be '{}'"
            .format(v_name, type(v_value), v_exp_t)
        ), *args)

class PlenumValueError(PlenumError, ValueError):
    """Wrapper exception for ValueError

    Extends ValueError to provide formatted error message

    :param v_name: variable name
    :param v_value: variable value
    :param v_exp_value: expected variable value
    """
    def __init__(self, v_name: str, v_value: Any, v_exp_value: Any, *args):
        super().__init__(
            ("invalid value for '{}' ({}), should be {}"
            .format(v_name, v_value, v_exp_value)
        ), *args)
