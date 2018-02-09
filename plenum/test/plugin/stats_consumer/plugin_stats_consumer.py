from plenum.common.types import EVENT_REQ_ORDERED, EVENT_NODE_STARTED, EVENT_PERIODIC_STATS_THROUGHPUT, \
    PLUGIN_TYPE_STATS_CONSUMER, EVENT_VIEW_CHANGE, EVENT_PERIODIC_STATS_LATENCIES, EVENT_PERIODIC_STATS_NODES, \
    EVENT_PERIODIC_STATS_TOTAL_REQUESTS, EVENT_PERIODIC_STATS_NODE_INFO, EVENT_PERIODIC_STATS_SYSTEM_PERFORMANCE_INFO
from typing import Dict, Any

from plenum.server.stats_consumer import StatsConsumer


class TestStatsConsumer(StatsConsumer):
    pluginType = PLUGIN_TYPE_STATS_CONSUMER

    def __init__(self):
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

    def sendStats(self, event: str, stats: Dict[str, Any]):
        assert event in {EVENT_REQ_ORDERED, EVENT_NODE_STARTED,
                         EVENT_PERIODIC_STATS_THROUGHPUT, EVENT_VIEW_CHANGE,
                         EVENT_PERIODIC_STATS_LATENCIES,
                         EVENT_PERIODIC_STATS_NODES,
                         EVENT_PERIODIC_STATS_TOTAL_REQUESTS,
                         EVENT_PERIODIC_STATS_NODE_INFO,
                         EVENT_PERIODIC_STATS_SYSTEM_PERFORMANCE_INFO}
        self._eventToFunc[event](stats)

    def _periodicStatsThroughput(self, stats: Dict[str, object]):
        assert stats

    def _sendStatsOnReqOrdered(self, stats: Dict[str, object]):
        assert stats.get("created_at")

    def _sendStatsOnNodeStart(self, stats: Dict[str, object]):
        assert stats.get("startedAtData")
        assert stats.get("throughputConfig")

    def _viewChange(self, viewChange: Dict[str, object]):
        assert "viewChange" in viewChange

    def _sendLatencies(self, latencies: Dict[str, object]):
        assert "masterLatency" in latencies
        assert "averageBackupLatency" in latencies

    def _sendKnownNodesInfo(self, nodes: Dict[str, object]):
        assert "connected" in nodes
        assert "disconnected" in nodes

    def _sendNodeInfo(self, nodeInfo: Dict[str, object]):
        assert 'name' in nodeInfo
        assert 'rank' in nodeInfo
        assert 'view' in nodeInfo
        assert 'creationDate' in nodeInfo
        assert 'ledger_dir' in nodeInfo
        assert 'keys_dir' in nodeInfo
        assert 'genesis_dir' in nodeInfo
        assert 'plugins_dir' in nodeInfo
        assert 'node_info_dir' in nodeInfo
        assert 'portN' in nodeInfo
        assert 'portC' in nodeInfo
        assert 'address' in nodeInfo

    def _sendSystemPerformanceInfo(self, performanceInfo: Dict[str, object]):
        assert 'cpu' in performanceInfo
        assert 'ram' in performanceInfo
        assert 'traffic' in performanceInfo

    def _sendTotalRequests(self, totalRequests: Dict[str, object]):
        assert "totalRequests" in totalRequests
