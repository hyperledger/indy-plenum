import os

from plenum.common.constants import PLUGIN_BASE_DIR_PATH
from plenum.common.config_util import getConfig
from plenum.server.plugin_loader import PluginLoader


class PluginLoaderHelper:

    @staticmethod
    def getPluginPath(name):
        config = getConfig()
        if PLUGIN_BASE_DIR_PATH in config.DefaultPluginPath:
            return os.path.join(config.DefaultPluginPath.get(
                PLUGIN_BASE_DIR_PATH), name)
        else:
            curPath = os.path.dirname(os.path.abspath(__file__))
            return os.path.join(curPath, name)

    @staticmethod
    def _getAllPlugins(allPluginPaths, *types):
        if not allPluginPaths:
            return {}
        else:
            allPlugins = {}
            for path in allPluginPaths:
                pl = PluginLoader(path)
                plugins = pl.plugins
                for key, value in plugins.items():
                    if types and key not in types:
                        continue
                    if key in allPlugins:
                        allPlugins[key].add(value)
                    else:
                        allPlugins[key] = value
            return allPlugins

    @staticmethod
    def getPluginsByType(pluginPaths, typ):
        return PluginLoaderHelper.getPluginsByTypes(pluginPaths, typ)[typ]

    """
    if types is not given, then, it will return whatever it founds in pluginPaths
    if types is given, then, it will either return those in the path or default (if configured)
    """
    @staticmethod
    def getPluginsByTypes(pluginPaths, *types):
        allPlugins = PluginLoaderHelper._getAllPlugins(pluginPaths, *types)

        if not types:
            return allPlugins

        finalPlugins = {}
        for typ in types:
            if typ not in allPlugins:
                finalPlugins[typ] = PluginLoaderHelper._getDefaultPluginsByType(
                    typ)
            else:
                finalPlugins[typ] = allPlugins[typ]
        return finalPlugins

    @staticmethod
    def _getDefaultPluginsByType(typ):
        config = getConfig()
        allPluginsPath = []

        if typ in config.DefaultPluginPath:
            allPluginsPath.append(PluginLoaderHelper.getPluginPath(
                config.DefaultPluginPath.get(typ)))
            allPlugins = PluginLoaderHelper._getAllPlugins(allPluginsPath)
            return allPlugins.get(typ, [])
        else:
            return []
