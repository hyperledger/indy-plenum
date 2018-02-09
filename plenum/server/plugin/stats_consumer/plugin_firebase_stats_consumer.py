from abc import abstractmethod
from typing import Dict

import jsonpickle

from plenum.common.types import EVENT_PERIODIC_STATS_THROUGHPUT, \
    EVENT_NODE_STARTED, EVENT_REQ_ORDERED, EVENT_PERIODIC_STATS_LATENCIES, \
    PLUGIN_TYPE_STATS_CONSUMER, EVENT_VIEW_CHANGE, EVENT_PERIODIC_STATS_NODES, \
    EVENT_PERIODIC_STATS_TOTAL_REQUESTS, EVENT_PERIODIC_STATS_NODE_INFO,\
    EVENT_PERIODIC_STATS_SYSTEM_PERFORMANCE_INFO
from stp_core.common.log import getlogger
from plenum.config import STATS_SERVER_IP, STATS_SERVER_PORT, STATS_SERVER_MESSAGE_BUFFER_MAX_SIZE
from plenum.server.plugin.stats_consumer.stats_publisher import StatsPublisher,\
    Topic
from plenum.server.plugin_loader import HasDynamicallyImportedModules
from plenum.server.stats_consumer import StatsConsumer

logger = getlogger()


class FirebaseStatsConsumer(StatsConsumer, HasDynamicallyImportedModules):
    pluginType = PLUGIN_TYPE_STATS_CONSUMER

    def __init__(self):
        super().__init__()
        self.statsPublisher = StatsPublisher(
            STATS_SERVER_IP, STATS_SERVER_PORT, STATS_SERVER_MESSAGE_BUFFER_MAX_SIZE)
        self._eventToFunc = {
            EVENT_REQ_ORDERED: self._sendStatsOnReqOrdered,
            EVENT_NODE_STARTED: self._sendStatsOnNodeStart,
            EVENT_PERIODIC_STATS_THROUGHPUT: self._periodicStatsThroughput,
            EVENT_VIEW_CHANGE: self._viewChange,
            EVENT_PERIODIC_STATS_LATENCIES: self._sendLatencies,
            EVENT_PERIODIC_STATS_NODES: self._sendKnownNodesInfo,
            EVENT_PERIODIC_STATS_TOTAL_REQUESTS: self._sendTotalRequests,
            EVENT_PERIODIC_STATS_NODE_INFO: self._sendNodeInfo,
            EVENT_PERIODIC_STATS_SYSTEM_PERFORMANCE_INFO: self._sendSystemPerformanceInfo}

    @abstractmethod
    def isModuleImportedSuccessfully(self):
        return True

    def _send(self, data: Dict[str, object]):
        self.statsPublisher.send(jsonpickle.dumps(data))

    def sendStats(self, event: str, stats: Dict[str, object]):
        self._eventToFunc[event](stats)

    def _periodicStatsThroughput(self, stats: Dict[str, object]):
        stats["eventName"] = str(Topic.PublishMtrStats)
        self._send(stats)

    def _sendStatsOnReqOrdered(self, stats: Dict[str, object]):
        stats["eventName"] = str(Topic.PublishAllStats)
        self._send(stats)

        # send total request to different metric
        if stats.get("hasMasterPrimary") == "Y":
            totalTransactions = dict(
                totalTransactions=stats.get('total requests'),
                eventName=str(Topic.PublishTotalTransactions)
            )
            self._send(totalTransactions)

    def _sendStatsOnNodeStart(self, stats: Dict[str, object]):
        startedAt = dict(
            startedAt=stats.get('startedAtData'),
            eventName=str(Topic.PublishStartedAt)
        )
        self._send(startedAt)

        config = dict(
            throughConfig=stats.get('throughputConfig'),
            eventName=str(Topic.PublishConfig)
        )
        self._send(config)

    def _viewChange(self, viewChange: Dict[str, object]):
        viewChange["eventName"] = str(Topic.PublishViewChange)
        self._send(viewChange)

    def _sendLatencies(self, latencies: Dict[str, object]):
        latencies["eventName"] = str(Topic.PublishLatenciesStats)
        self._send(latencies)

    def _sendKnownNodesInfo(self, nodes: Dict[str, object]):
        nodes["eventName"] = str(Topic.PublishNodestackStats)
        self._send(nodes)

    def _sendNodeInfo(self, nodeInfo: Dict[str, object]):
        nodeInfo["eventName"] = str(Topic.PublishNodeStats)
        self._send(nodeInfo)

    def _sendSystemPerformanceInfo(self, performanceInfo: Dict[str, object]):
        performanceInfo["eventName"] = str(Topic.PublishSystemStats)
        self._send(performanceInfo)

    def _sendTotalRequests(self, totalRequests: Dict[str, object]):
        totalRequests["eventName"] = str(Topic.PublishTotalRequestsStats)
        self._send(totalRequests)
