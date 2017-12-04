import os
from typing import Tuple
from importlib import import_module
from importlib.util import module_from_spec, spec_from_file_location
from stp_core.common.config.util import getConfig as STPConfig


CONFIG = None


def getInstalledConfig(installDir, configFile):
    """
    Reads config from the installation directory of Plenum.

    :param installDir: installation directory of Plenum
    :param configFile: name of the configuration file
    :raises: FileNotFoundError
    :return: the configuration as a python object
    """
    configPath = os.path.join(installDir, configFile)
    if not os.path.exists(configPath):
        raise FileNotFoundError("No file found at location {}".
                                format(configPath))
    spec = spec_from_file_location(configFile, configPath)
    config = module_from_spec(spec)
    spec.loader.exec_module(config)
    return config


def extend_with_external_config(extendee: object, extender: Tuple[str, str], required: bool = False) -> object:
    try:
        config = getInstalledConfig(*extender)
        extendee.__dict__.update(config.__dict__)
    except FileNotFoundError as err:
        if required:
            raise err
    return extendee


def extend_with_default_external_config(extendee: object, user_config_dir: str = None) -> object:
    extend_with_external_config(extendee, (extendee.GENERAL_CONFIG_DIR, extendee.GENERAL_CONFIG_FILE))

    # fail if network is not set
    if not extendee.NETWORK_NAME:
        return
        # raise Exception('NETWORK_NAME must be set')

    network_config_dir = os.path.join(extendee.GENERAL_CONFIG_DIR,
                                      extendee.NETWORK_NAME)
    extend_with_external_config(extendee,
                                (network_config_dir,
                                 extendee.NETWORK_CONFIG_FILE))

    if not user_config_dir:
        user_config_dir = os.path.join(extendee.baseDir, extendee.NETWORK_NAME)
    user_config_dir = os.path.expanduser(user_config_dir)
    extend_with_external_config(extendee,
                                (user_config_dir,
                                 extendee.USER_CONFIG_FILE))


def getConfig(user_config_dir=None):
    """
    Reads a file called config.py in the project directory

    :raises: FileNotFoundError
    :return: the configuration as a python object
    """
    global CONFIG
    if not CONFIG:
        stp_config = STPConfig(user_config_dir)
        plenum_config = import_module("plenum.config")
        config = stp_config
        config.__dict__.update(plenum_config.__dict__)

        extend_with_default_external_config(config, user_config_dir)

        # "unsafe" is a set of attributes that can set certain behaviors that
        # are not safe, for example, 'disable_view_change' disables view changes
        # from happening. This might be useful in testing scenarios, but never
        # in a live network.
        if not hasattr(config, 'unsafe'):
            setattr(config, 'unsafe', set())
        CONFIG = config
    return CONFIG
