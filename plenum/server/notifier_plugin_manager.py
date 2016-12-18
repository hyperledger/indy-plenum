import pip
import importlib

from plenum.common.log import getlogger

logger = getlogger()


class PluginManager:
    prefix = 'sovrinnotifier'
    __instance = None

    def __new__(cls):
        if PluginManager.__instance is None:
            PluginManager.__instance = object.__new__(cls)
        return PluginManager.__instance

    def __init__(self):
        self.plugins = []
        self.importPlugins()

    def importPlugins(self):
        plugins = self.findPlugins()
        for plugin in plugins:
            try:
                module = importlib.import_module(plugin)
                self.plugins.append(module)
            except Exception as e:
                logger.error('Importing module {} failed due to {}'
                             .format(plugin, e))

    def sendMessage(self, topic, message):
        for plugin in self.plugins:
            try:
                plugin.sendMessage(topic, message)
            except Exception as e:
                logger.error('Sending message failed for plugin {} due to {}'
                             .format(plugin.name, e))

    def findPlugins(self):
        return [pkg.key
                for pkg in pip.utils.get_installed_distributions()
                if pkg.key.startswith(PluginManager.prefix)]