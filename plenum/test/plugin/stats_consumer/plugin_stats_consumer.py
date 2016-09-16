from plenum.common.types import EVENT_REQ_ORDERED, EVENT_NODE_STARTED, EVENT_PERIODIC_STATS_THROUGHPUT, \
    PLUGIN_TYPE_STATS_CONSUMER, EVENT_VIEW_CHANGE, EVENT_PERIODIC_STATS_LATENCIES
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
            EVENT_PERIODIC_STATS_LATENCIES: self._sendLatencies
        }

    def sendStats(self, event: str, stats: Dict[str, Any]):
        assert event in {EVENT_REQ_ORDERED, EVENT_NODE_STARTED,
                         EVENT_PERIODIC_STATS_THROUGHPUT, EVENT_VIEW_CHANGE,
                         EVENT_PERIODIC_STATS_LATENCIES}
        self._eventToFunc[event](stats)

    def _periodicStatsThroughput(self, stats: Dict[str, object]):
        assert stats

    def _sendStatsOnReqOrdered(self, stats: Dict[str, object]):
        assert stats.get("created_at")
        if stats.get("hasMasterPrimary") == "Y":
            assert stats.get("total requests")

    def _sendStatsOnNodeStart(self, stats: Dict[str, object]):
        assert stats.get("startedAtData")
        assert stats.get("throughputConfig")

    def _viewChange(self, viewChange: Dict[str, object]):
        assert "viewChange" in viewChange

    def _sendLatencies(self, latencies: Dict[str, object]):
        assert "masterLatency" in latencies
        assert "averageBackupLatency" in latencies
