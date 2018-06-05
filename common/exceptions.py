# -*- coding: utf-8 -*-
from typing import Any

class PlenumError(Exception):
    """Base exceptions class for Plenum exceptions"""
    pass


class PlenumTypeError(PlenumError, TypeError):
    """Wrapper exception for TypeError

    Extends TypeError to provide formatted error message

    :param vname: variable name
    :param vvalue: variable value
    :param vtype: expected variable type
    """
    def __init__(self, vname: str, vvalue: Any, vtype: Any, *args):
        super().__init__(
            ("incorrect type of '{}' ({}), should be '{}'"
            .format(vname, type(vvalue), vtype)
        ), *args)
