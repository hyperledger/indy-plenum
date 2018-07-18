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

        _d = set(('value', 'cnt')) - set(historicalData.keys())
        if _d:
            raise KeyError(
                "{} keys are not found in 'historicalData'".format(_d)
            )

        _d = set(('min_cnt', 'bounds_coeff', 'min_activity_threshold',
                  'use_weighted_bounds_coeff', 'enabled')) - set(config.keys())
        if _d:
            raise KeyError(
                "{} keys are not found in 'config'".format(_d)
            )

        if not (enabled and config['enabled']):
            logger.trace('Suspicious Spike check is disabled')
            return None

        min_cnt = config['min_cnt']
        val_thres = config['min_activity_threshold']
        bounds_coeff = config['bounds_coeff']
        use_weighted_bounds_coeff = config['use_weighted_bounds_coeff']

        val = historicalData['value']
        alpha = 2 / (min_cnt + 1)
        historicalData['value'] = val * (1 - alpha) + newVal * alpha
        historicalData['cnt'] += 1
        cnt = historicalData['cnt']

        if cnt <= min_cnt:
            logger.debug('Not enough data to detect a {} spike'.format(event))
            return None

        if val < val_thres:
            logger.debug('Current activity {} is below threshold level {}'.format(val, val_thres))
            return None

        log_base = 10
        if use_weighted_bounds_coeff and cnt > log_base:
            # Weighted coefficient allows to adapt bounds in accordance to values,
            # growing values leads to lower bounds.
            bounds_coeff /= math.log(cnt, log_base)

        lower_bound = val / bounds_coeff
        higher_bound = val * bounds_coeff
        if lower_bound <= newVal <= higher_bound:
            logger.debug(
                '{}: Actual value {} is within bounds [{}, {}]. Expected value: {}'.format(
                    event, newVal, lower_bound, higher_bound, val))
            return None

        message = '{} suspicious spike has been noticed on node {} at {}. ' \
                  'Actual: {}. Expected: {}. Bounds: [{}, {}].'\
            .format(event, nodeName, time.time(), newVal, val, lower_bound, higher_bound)
        logger.display(message)
        return self._sendMessage(event, message)

    def importPlugins(self):
        plugins = self._findPlugins()
        logger.info("Found notifier plugins: {}".format(plugins))
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
