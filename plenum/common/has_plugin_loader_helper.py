from plenum.server.plugin_loader import PluginLoader


class PluginLoaderHelper:

    @staticmethod
    def _getPlugins(pluginPath, pluginType):
        if pluginPath:
            pl = PluginLoader(pluginPath)
            return pl.plugins[pluginType]
        else:
            return []