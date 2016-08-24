import importlib

import os

from plenum.common.util import getConfig


def loadPlugins(baseDir):
    config = getConfig()
    pluginsDirPath = os.path.expanduser(os.path.join(baseDir, config.PluginsDir))
    i = 0
    if os.path.exists(pluginsDirPath):
        for pluginName in config.PluginsToLoad:
            pluginPath = os.path.join(pluginsDirPath, pluginName + ".py")
            spec = importlib.util.spec_from_file_location(pluginName,
                                                          pluginPath)
            plugin = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(plugin)
            i += 1

    print("total plugins loaded: {}".format(i))
    return i
