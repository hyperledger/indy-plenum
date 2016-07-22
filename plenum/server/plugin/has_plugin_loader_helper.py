import os

from plenum.common.types import PLUGIN_TYPE_STATS_CONSUMER
from plenum.server.plugin_loader import PluginLoader


class PluginLoaderHelper:


    @staticmethod
    def _pluginPath(name):
        curPath = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(curPath, name)

    @staticmethod
    def _getAllPlugins(allPluginPaths):
        if not allPluginPaths:
            return {}
        else:
            allPlugins = {}
            for path in allPluginPaths:
                pl = PluginLoader(path)
                plugins = pl.plugins
                for key, value in plugins.items():
                    if key in allPlugins:
                        allPlugins[key].add(value)
                    else:
                        allPlugins[key] = value
            return allPlugins


    @staticmethod
    def getPluginsByType(type, pluginPaths):
        allPlugins = PluginLoaderHelper._getAllPlugins(pluginPaths)
        if type in allPlugins:
            return allPlugins[type]
        else:
            # in case explicit path not provided for particular type,
            # as of now, it will try to load default plugins (if provided)
            # but, if we decide not to load default plugins
            # then, instead of calling below function, just return empty list like: return []
            return PluginLoaderHelper._getDefaultPluginsByType(type)


    @staticmethod
    def _getDefaultPluginsByType(type):
        allPluginsPath = []
        if type == PLUGIN_TYPE_STATS_CONSUMER:
            allPluginsPath.append(PluginLoaderHelper._pluginPath('stats_consumer'))

        allPlugins = PluginLoaderHelper._getAllPlugins(allPluginsPath)

        if type in allPlugins:
            return allPlugins[type]
        else:
            return []

