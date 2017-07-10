import os
from importlib import import_module
from importlib.util import module_from_spec, spec_from_file_location


# TODO: this is a stub, remove it when new
# configuration management tool is used

CONFIG = None


def getConfig(homeDir=None):
    """
    Reads a file called config.py in the project directory

    :raises: FileNotFoundError
    :return: the configuration as a python object
    """
    global CONFIG
    if not CONFIG:
        CONFIG = import_module("stp_core.config")
    return CONFIG
