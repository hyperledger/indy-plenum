import pip
import importlib
import math
from typing import Dict
import time

from stp_core.common.log import getlogger

logger = getlogger()


notifierPluginTriggerEvents = {
    'nodeRequestSpike': 'NodeRequestSuspiciousSpike',
    'clusterThroughputSpike': 'ClusterThroughputSuspiciousSpike',
    # TODO: Implement clusterLatencyTooHigh event triggering
    'clusterLatencyTooHigh': 'ClusterLatencyTooHigh',
    'nodeUpgradeScheduled': 'NodeUpgradeScheduled',
    'nodeUpgradeComplete': 'NodeUpgradeComplete',
    'nodeUpgradeFail': 'NodeUpgradeFail',
    'poolUpgradeCancel': 'PoolUpgradeCancel'
}


class PluginManager:
    prefix = 'indynotifier'
    __instance = None

    def __new__(cls):
        if PluginManager.__instance is None:
            PluginManager.__instance = object.__new__(cls)
        return PluginManager.__instance

    def __init__(self):
        self.plugins = []
        self.topics = notifierPluginTriggerEvents
        self.importPlugins()

    def sendMessageUponNodeUpgradeScheduled(
            self, message='Node uprgade has been scheduled'):
        return self._sendMessage(self.topics['nodeUpgradeScheduled'], message)

    def sendMessageUponNodeUpgradeComplete(
            self, message='Node has successfully upgraded.'):
        return self._sendMessage(self.topics['nodeUpgradeComplete'], message)

    def sendMessageUponNodeUpgradeFail(
            self, message='Node upgrade has failed. Please take action.'):
        return self._sendMessage(self.topics['nodeUpgradeFail'], message)

    def sendMessageUponPoolUpgradeCancel(
            self, message='Pool upgrade has been cancelled. Please take action.'):
        return self._sendMessage(self.topics['poolUpgradeCancel'], message)

    def sendMessageUponSuspiciousSpike(
            self,
            event: str,
            historicalData: Dict,
            newVal: float,
            config: Dict,
            nodeName: str,
            enabled: bool):
        assert 'value' in historicalData
        assert 'cnt' in historicalData

        assert 'min_cnt' in config
        assert 'borders_coeff' in config
        assert 'min_activity_threshold' in config
        assert 'use_weighted_borders_coeff' in config
        assert 'enabled' in config

        if not (enabled and config['enabled']):
            logger.trace('Suspicious Spike check is disabled')
            return None

        min_cnt = config['min_cnt']
        val_thres = config['min_activity_threshold']
        borders_coeff = config['borders_coeff']
        use_weighted_borders_coeff = config['use_weighted_borders_coeff']

        val = historicalData['value']
        alpha = 2 / (min_cnt + 1)
        historicalData['value'] = val * (1 - alpha) + newVal * alpha
        historicalData['cnt'] += 1
        cnt = historicalData['cnt']

        if val < val_thres:
            logger.debug('Current activity {} is below threshold level {}'.format(val, val_thres))
            return None

        if cnt <= min_cnt:
            logger.debug('Not enough data to detect a {} spike'.format(event))
            return None

        log_base = 10
        if use_weighted_borders_coeff and cnt > log_base:
            # Weighted coefficient allows to adapt borders in accordance to values,
            # growing values leads to lower borders.
            borders_coeff /= math.log(cnt, log_base)
        if (val / borders_coeff) <= newVal <= (val * borders_coeff):
            logger.debug(
                '{}: New value {} is within bounds. Average: {}'.format(
                    event, newVal, val))
            return None

        message = '{} suspicious spike has been noticed on node {} at {}. ' \
                  'Usual: {}. New: {}.'\
            .format(event, nodeName, time.time(), val, newVal)
        logger.debug(message)
        return self._sendMessage(event, message)

    def importPlugins(self):
        plugins = self._findPlugins()
        self.plugins = []
        i = 0
        for plugin in plugins:
            try:
                module = importlib.import_module(plugin)
                self.plugins.append(module)
                i += 1
                logger.info(
                    "Successfully imported Notifier Plugin: {}".format(plugin))
            except Exception as e:
                logger.error(
                    'Importing Notifier Plugin {} failed due to {}'.format(
                        plugin, e))
        return i, len(plugins)

    def _sendMessage(self, topic, message):
        i = 0
        for plugin in self.plugins:
            try:
                plugin.send_message(topic, message)
                i += 1
            except Exception as e:
                logger.error('Sending message failed for plugin {} due to {}'
                             .format(plugin.__name__, e))
        return i, len(self.plugins)

    def _findPlugins(self):
        return [pkg.key
                for pkg in pip.utils.get_installed_distributions()
                if pkg.key.startswith(PluginManager.prefix)]
