import inspect
import re
import sys
from abc import abstractmethod
from importlib import import_module
from os import listdir
from os.path import isfile, join
from typing import Any, Set, Dict

from stp_core.common.log import getlogger

from plenum.common.types import PLUGIN_TYPE_VERIFICATION, \
    PLUGIN_TYPE_STATS_CONSUMER

logger = getlogger()


class HasDynamicallyImportedModules:

    @abstractmethod
    def isModuleImportedSuccessfully(self):
        raise NotImplementedError


class PluginLoader:
    """
    The PluginLoader loads plugins in a provided directory.

    It first scans the directory for any file that starts with the word 'Plugin'
    or 'plugin', and ends with .py or .pyc. It imports these modules. Then it
    will look for any classes declared in these modules that have a class
    attribute named 'pluginType', and then it checks to see that the
    'pluginType' attribute is equal to any of the valid types. Right now there
    three types of plugins: 'VERIFICATION', 'PROCESSING' and STATS_CONSUMER.
    Then,it instantiates an object of that class type, without any constructor
    arguments. This is the plugin instance.

    When an opVerificationPluginPath keyword argument is provided in the Node
    constructor, the Node will create a PluginLoader with that path and look
    for all VERIFICATION plugins. Then, when the Node receives a client request,
    it calls the verify(msg) method of the plugin instance(s). The verify method
    of the VERIFICATION plugin should raise an exception if the msg is not
    valid.

    Example plugin, in a file named 'plugin_name_verifier.py':

    class NameVerifier:
    pluginType = 'VERIFICATION'

    @staticmethod
    def verify(operation):
        assert len(operation['name']) <= 50, 'name too long'

    """

    def __init__(self, path):
        if not path:
            raise ValueError("path is required")
        self.path = path
        self._validTypes = [PLUGIN_TYPE_VERIFICATION,
                            PLUGIN_TYPE_STATS_CONSUMER]
        self._pluginTypeAttrName = 'pluginType'
        self.plugins = self._load()

    def get(self, name):
        """Retrieve a plugin by name."""
        try:
            return self.plugins[name]
        except KeyError:
            raise RuntimeError("plugin {} does not exist".format(name))

    def _getModules(self):
        c = re.compile("([pP]lugin.+)\.py(c?)$")
        matches = [c.match(f) for f in listdir(self.path)
                   if isfile(join(self.path, f))]
        mods = [m.groups()[0] for m in matches if m]
        for m in mods:
            if m in sys.modules:
                logger.debug("skipping plugin {} because it is already "
                             "loaded".format(m))
        return mods

    def _load(self) -> Dict[str, Set[Any]]:
        mods = self._getModules()
        plugins = {}

        if mods:
            sys.path.insert(0, self.path)
            for mod in mods:
                m = import_module(mod)
                classes = [cls for cls in m.__dict__.values()
                           if inspect.isclass(cls)]
                for c in classes:
                    if not hasattr(c, self._pluginTypeAttrName):
                        logger.display("skipping plugin {}[class: {}] because it does not have a '{}' "
                                       "attribute".format(mod, c, self._pluginTypeAttrName))
                    else:
                        typ = c.pluginType
                        if typ not in self._validTypes:
                            logger.display("skipping plugin '{0}' because it does not have a valid '{1}' "
                                           "attribute; valid {1} are: {2}".
                                           format(mod, self._pluginTypeAttrName, self._validTypes))
                        else:
                            inst = c()
                            if isinstance(inst, HasDynamicallyImportedModules):
                                importSuccessful = inst.isModuleImportedSuccessfully()
                            else:
                                importSuccessful = True

                            if importSuccessful:
                                logger.info("plugin {} successfully loaded "
                                            "from module {}".
                                            format(c.__name__, mod),
                                            extra={"cli": True})
                                if typ in plugins:
                                    plugins[typ].add(inst)
                                else:
                                    plugins[typ] = {inst}
                            else:
                                logger.info("** ERROR occurred while loading {} plugin from module {}".
                                            format(c.__name__, mod))

            sys.path.pop(0)

        if not plugins:
            logger.warning("no plugins found in {}".format(self.path),
                           extra={"cli": "WARNING"})
        return plugins
