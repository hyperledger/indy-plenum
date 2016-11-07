import os
from importlib import import_module
from importlib._bootstrap import module_from_spec
from importlib._bootstrap_external import spec_from_file_location

import plenum.common

CONFIG = None


def getInstalledConfig(installDir, configFile):
    """
    Reads config from the installation directory of Plenum.

    :param installDir: installation directory of Plenum
    :param configFile: name of the confiuration file
    :raises: FileNotFoundError
    :return: the configuration as a python object
    """
    configPath = os.path.join(installDir, configFile)
    if os.path.exists(configPath):
        spec = spec_from_file_location(configFile,
                                                      configPath)
        config = module_from_spec(spec)
        spec.loader.exec_module(config)
        return config
    else:
        raise FileNotFoundError("No file found at location {}".format(configPath))


def getConfig(homeDir=None):
    """
    Reads a file called config.py in the project directory

    :raises: FileNotFoundError
    :return: the configuration as a python object
    """
    if not plenum.common.config_util.CONFIG:
        refConfig = import_module("plenum.config")
        try:
            homeDir = os.path.expanduser(homeDir or "~")

            configDir = os.path.join(homeDir, ".plenum")
            config = getInstalledConfig(configDir, "plenum_config.py")

            refConfig.__dict__.update(config.__dict__)
        except FileNotFoundError:
            pass
        refConfig.baseDir = os.path.expanduser(refConfig.baseDir)
        plenum.common.config_util.CONFIG = refConfig
    return plenum.common.config_util.CONFIG