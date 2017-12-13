from plenum.common.types import PLUGIN_TYPE_VERIFICATION, PLUGIN_TYPE_PROCESSING, PLUGIN_TYPE_STATS_CONSUMER
from plenum.server.plugin.has_plugin_loader_helper import PluginLoaderHelper
from plenum.test.plugin.conftest import AUCTION_REQ_VALIDATION_PLUGIN_PATH_VALUE, \
    AUCTION_REQ_PROCESSOR_PLUGIN_PATH_VALUE
from plenum.test.plugin.helper import getPluginPath


def assertPluginCounts(typ, pluginPaths, expectedPlugins):
    totalPlugins = PluginLoaderHelper.getPluginsByType(pluginPaths, typ)
    assert len(totalPlugins) == expectedPlugins


def testPluginLoaderHelper():
    assertPluginCounts(PLUGIN_TYPE_VERIFICATION, None, 0)
    assertPluginCounts(PLUGIN_TYPE_PROCESSING, None, 0)
    assertPluginCounts(PLUGIN_TYPE_STATS_CONSUMER, None, 1)

    assertPluginCounts(PLUGIN_TYPE_VERIFICATION, [], 0)
    assertPluginCounts(PLUGIN_TYPE_PROCESSING, [], 0)
    assertPluginCounts(PLUGIN_TYPE_STATS_CONSUMER, [], 1)

    auctionReqValidPluginPath = getPluginPath(
        AUCTION_REQ_VALIDATION_PLUGIN_PATH_VALUE)
    pluginPaths = [auctionReqValidPluginPath]
    assertPluginCounts(PLUGIN_TYPE_VERIFICATION, pluginPaths, 1)
    assertPluginCounts(PLUGIN_TYPE_PROCESSING, pluginPaths, 0)
    assertPluginCounts(PLUGIN_TYPE_STATS_CONSUMER, pluginPaths, 1)

    auctionReqProcPluginPath = getPluginPath(
        AUCTION_REQ_PROCESSOR_PLUGIN_PATH_VALUE)
    pluginPaths = [auctionReqValidPluginPath, auctionReqProcPluginPath]
    assertPluginCounts(PLUGIN_TYPE_VERIFICATION, pluginPaths, 1)
    assertPluginCounts(PLUGIN_TYPE_PROCESSING, pluginPaths, 1)
    assertPluginCounts(PLUGIN_TYPE_STATS_CONSUMER, pluginPaths, 1)
