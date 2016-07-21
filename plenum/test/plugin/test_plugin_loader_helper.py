from plenum.common.has_plugin_loader_helper import PluginLoaderHelper
from plenum.common.types import PLUGIN_TYPE_VERIFICATION, PLUGIN_TYPE_PROCESSING
from plenum.test.plugin.conftest import AUCTION_REQ_VALIDATION_PLUGIN_PATH_VALUE, \
    AUCTION_REQ_PROCESSOR_PLUGIN_PATH_VALUE
from plenum.test.plugin.helper import pluginPath


def testPluginLoaderHelper():
    pluginPaths = None
    totalPlugins = PluginLoaderHelper.getPluginsByType(PLUGIN_TYPE_VERIFICATION, pluginPaths)
    assert len(totalPlugins) == 0

    pluginPaths = []
    totalPlugins = PluginLoaderHelper.getPluginsByType(PLUGIN_TYPE_VERIFICATION, pluginPaths)
    assert len(totalPlugins) == 0

    auctionReqValidPluginPath = pluginPath(AUCTION_REQ_VALIDATION_PLUGIN_PATH_VALUE)
    pluginPaths = [auctionReqValidPluginPath]
    totalPlugins = PluginLoaderHelper.getPluginsByType(PLUGIN_TYPE_VERIFICATION, pluginPaths)
    assert len(totalPlugins) == 1

    auctionReqProcPluginPath = pluginPath(AUCTION_REQ_PROCESSOR_PLUGIN_PATH_VALUE)
    pluginPaths = [auctionReqValidPluginPath, auctionReqProcPluginPath]
    totalPlugins = PluginLoaderHelper.getPluginsByType(PLUGIN_TYPE_VERIFICATION, pluginPaths)
    assert len(totalPlugins) == 1
    totalPlugins = PluginLoaderHelper.getPluginsByType(PLUGIN_TYPE_PROCESSING, pluginPaths)
    assert len(totalPlugins) == 1