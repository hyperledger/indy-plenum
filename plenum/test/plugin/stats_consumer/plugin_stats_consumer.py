from plenum.common.types import EVENT_REQ_ORDERED, EVENT_NODE_STARTED, EVENT_PERIODIC_STATS_THROUGHPUT, \
    PLUGIN_TYPE_STATS_CONSUMER
from typing import Dict, Any

from plenum.server.stats_consumer import StatsConsumer


class TestStatsConsumer(StatsConsumer):
    pluginType = PLUGIN_TYPE_STATS_CONSUMER

    def __init__(self):
        self._eventToFunc = {
            EVENT_REQ_ORDERED: self._sendStatsOnReqOrdered,
            EVENT_NODE_STARTED: self._sendStatsOnNodeStart,
            EVENT_PERIODIC_STATS_THROUGHPUT: self._periodicStatsThroughput,
        }

    def sendStats(self, event: str, stats: Dict[str, Any]):
        print("Test Stats Consumer Plugin: event: {} => stats: {}".format(event, stats))
        assert event in {EVENT_REQ_ORDERED, EVENT_NODE_STARTED, EVENT_PERIODIC_STATS_THROUGHPUT}
        self._eventToFunc[event](stats)

    def _periodicStatsThroughput(self, stats: Dict[str, object]):
        assert stats

    def _sendStatsOnReqOrdered(self, stats: Dict[str, object]):
        assert stats.get("created_at")
        if stats.get("hasMasterPrimary") == "Y":
            assert stats.get("total requests")

    def _sendStatsOnNodeStart(self, stats: Dict[str, object]):
        assert stats.get("startedAtData")
        assert stats.get("throughputData")

