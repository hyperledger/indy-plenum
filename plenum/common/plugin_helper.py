import importlib

import os

from plenum.common.util import getConfig


def loadPlugins(baseDir):
    print("Plugin loading request came to load plugins from basedir: {}".format(baseDir))
    config = getConfig()
    pluginsDirPath = os.path.expanduser(os.path.join(baseDir, config.PluginsDir))
    i = 0
    if os.path.exists(pluginsDirPath):
        for pluginName in config.PluginsToLoad:
            pluginPath = os.path.join(pluginsDirPath, pluginName + ".py")
            if os.path.exists(pluginPath):
                spec = importlib.util.spec_from_file_location(pluginName,
                                                              pluginPath)
                plugin = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(plugin)
                i += 1
            else:
                print("** Note: Plugin file does not exists: {}. "
                      "Create plugin file if you want to load it".format(pluginPath))
    else:
        print("** Note: Plugins directory does not exists: {}. "
              "Create plugin directory and plugin files if you want to load any plugins".format(pluginsDirPath))

    print("Total plugins loaded from basedir {} are : {}".format(baseDir, i))
    return i
