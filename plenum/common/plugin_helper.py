from importlib.util import module_from_spec, spec_from_file_location
import os

from stp_core.common.log import getlogger

pluginsLoaded = {}  # Dict(plugins_dir, List[plugin names])
pluginsNotFound = {}  # Dict(plugins_dir, List[plugin names])

logger = getlogger("plugin-loader")


def loadPlugins(plugins_dir, plugins_to_load=None):
    global pluginsLoaded

    alreadyLoadedPlugins = pluginsLoaded.get(plugins_dir)
    i = 0
    if alreadyLoadedPlugins:
        logger.debug("Plugins {} are already loaded from plugins_dir: {}".format(
            alreadyLoadedPlugins, plugins_dir))
    else:
        logger.debug(
            "Plugin loading started to load plugins from plugins_dir: {}".format(
                plugins_dir))

        if not os.path.exists(plugins_dir):
            os.makedirs(plugins_dir)
            logger.debug("Plugin directory created at: {}".format(
                plugins_dir))

        if plugins_to_load is not None:
            for pluginName in plugins_to_load:
                pluginPath = os.path.expanduser(
                    os.path.join(plugins_dir, pluginName + ".py"))
                try:
                    if os.path.exists(pluginPath):
                        spec = spec_from_file_location(
                            pluginName,
                            pluginPath)
                        plugin = module_from_spec(spec)
                        spec.loader.exec_module(plugin)
                        if plugins_dir in pluginsLoaded:
                            pluginsLoaded[plugins_dir].add(pluginName)
                        else:
                            pluginsLoaded[plugins_dir] = {pluginName}
                        i += 1
                    else:
                        if not pluginsNotFound.get(pluginPath):
                            logger.error("Note: Plugin file does not exists: {}. "
                                         "Create plugin file if you want to load it"
                                         .format(pluginPath), extra={"cli": False})
                            pluginsNotFound[pluginPath] = "Notified"

                except Exception as ex:
                    # TODO: Is this strategy ok to catch any exception and
                    # just print the error and continue,
                    # or it should fail if there is error in plugin loading
                    logger.error("** Error occurred during loading plugin {}: {}".format(pluginPath, str(ex)))

    logger.debug(
        "Total plugins loaded from plugins_dir {} are : {}".format(plugins_dir, i))
    return i
