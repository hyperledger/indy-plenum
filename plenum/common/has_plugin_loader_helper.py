from plenum.server.plugin_loader import PluginLoader


class PluginLoaderHelper:

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
            return []