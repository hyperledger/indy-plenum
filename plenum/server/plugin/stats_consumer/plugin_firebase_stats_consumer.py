from datetime import datetime
from functools import partial
from typing import Dict

import jsonpickle
from firebase import firebase
from firebase.async import get_process_pool
from firebase.lazy import LazyLoadProxy

from plenum.common.types import EVENT_PERIODIC_STATS_THROUGHPUT, EVENT_NODE_STARTED, EVENT_REQ_ORDERED, \
    PLUGIN_TYPE_STATS_CONSUMER
from plenum.server.stats_consumer import StatsConsumer

# Temporary fix for letting firebase create only 1 extra process
# TODO: This needs to be some kind of configuration option
firebase.process_pool.terminate()
from firebase import async
async._process_pool = None
firebase.process_pool = LazyLoadProxy(partial(get_process_pool, 1))


class FirebaseStatsConsumer(StatsConsumer):
    pluginType = PLUGIN_TYPE_STATS_CONSUMER

    def __init__(self):
        self._firebaseClient = None

        self._eventToFunc = {
            EVENT_REQ_ORDERED: self._sendStatsOnReqOrdered,
            EVENT_NODE_STARTED: self._sendStatsOnNodeStart,
            EVENT_PERIODIC_STATS_THROUGHPUT: self._periodicStatsThroughput,
        }

    @property
    def firebaseClient(self):
        if self._firebaseClient:
            return self._firebaseClient
        else:
            self._firebaseClient = firebase.FirebaseApplication(
                "https://plenumstats.firebaseio.com/", None)
            return self._firebaseClient


    def sendStats(self, event: str, stats: Dict[str, object]):
        self._eventToFunc[event](stats)


    def _periodicStatsThroughput(self, stats: Dict[str, object]):
        self.firebaseClient.post_async(url="/mtr_stats", data=stats,
                                       callback=lambda response: None,
                                       params={'print': 'silent'},
                                       headers={'Connection': 'keep-alive'},
                                       )

    def _sendStatsOnReqOrdered(self, stats: Dict[str, object]):
        metrics = jsonpickle.loads(jsonpickle.dumps(dict(stats)))
        metrics["created_at"] = datetime.utcnow().isoformat()
        self.firebaseClient.post_async(url="/all_stats", data=metrics,
                                  callback=lambda response: None,
                                  params={'print': 'silent'},
                                  headers={'Connection': 'keep-alive'},
                                  )

        # send total request to different metric
        if stats.get("hasMasterPrimary") == "Y":
            self.firebaseClient.put_async(url="/totalTransactions",
                                 name="totalTransactions",
                                 data=stats.get('total requests'),
                                 callback=lambda response: None,
                                 params={'print': 'silent'},
                                 headers={'Connection': 'keep-alive'},
                                 )

    def _sendStatsOnNodeStart(self, stats: Dict[str, object]):

        self.firebaseClient.put_async(url="/startedAt", name="startedAt",
                                 data=stats.get('startedAtData'),
                                 callback=lambda response: None,
                                 params={'print': 'silent'},
                                 headers={'Connection': 'keep-alive'},
                                 )

        self.firebaseClient.put_async(url="/config", name="throughput",
                                  data=stats.get('throughputData'),
                                  callback=lambda response: None,
                                  params={'print': 'silent'},
                                  headers={'Connection': 'keep-alive'},
                                  )




# import importlib
# from datetime import datetime
# from functools import partial
# from typing import Dict
#
# import jsonpickle
#
# from plenum.common.util import getlogger
# from plenum.common.types import EVENT_PERIODIC_STATS_THROUGHPUT, EVENT_NODE_STARTED, EVENT_REQ_ORDERED, \
#     PLUGIN_TYPE_STATS_CONSUMER
# from plenum.server.stats_consumer import StatsConsumer
#
#
# logger = getlogger()
# firebaseModule = None
# try:
#     firebaseModule = importlib.import_module("firebase")
#     firebaseModule.process_pool.terminate()
#     firebaseModule.async._process_pool = None
#     # Temporary fix for letting firebase create only 1 extra process
#     # TODO: This needs to be some kind of configuration option
#     firebaseModule.process_pool = firebaseModule.lazy.LazyLoadProxy(partial(firebaseModule.async.__dict__.get('get_process_pool'), 1))
# except ImportError:
#     logger.warn("firebase plugin not found, please install it and then retry")
#
#
# class FirebaseStatsConsumer(StatsConsumer):
#     pluginType = PLUGIN_TYPE_STATS_CONSUMER
#
#     def __init__(self):
#         self._firebaseClient = None
#
#         self._eventToFunc = {
#             EVENT_REQ_ORDERED: self._sendStatsOnReqOrdered,
#             EVENT_NODE_STARTED: self._sendStatsOnNodeStart,
#             EVENT_PERIODIC_STATS_THROUGHPUT: self._periodicStatsThroughput,
#         }
#
#     @property
#     def firebaseClient(self):
#         if self._firebaseClient:
#             return self._firebaseClient
#         else:
#             self._firebaseClient = firebaseModule.FirebaseApplication(
#                 "https://plenumstats.firebaseio.com/", None)
#             return self._firebaseClient
#
#
#     def sendStats(self, event: str, stats: Dict[str, object]):
#         self._eventToFunc[event](stats)
#
#
#     def _periodicStatsThroughput(self, stats: Dict[str, object]):
#         self.firebaseClient.post_async(url="/mtr_stats", data=stats,
#                                        callback=lambda response: None,
#                                        params={'print': 'silent'},
#                                        headers={'Connection': 'keep-alive'},
#                                        )
#
#     def _sendStatsOnReqOrdered(self, stats: Dict[str, object]):
#         metrics = jsonpickle.loads(jsonpickle.dumps(dict(stats)))
#         metrics["created_at"] = datetime.utcnow().isoformat()
#         self.firebaseClient.post_async(url="/all_stats", data=metrics,
#                                   callback=lambda response: None,
#                                   params={'print': 'silent'},
#                                   headers={'Connection': 'keep-alive'},
#                                   )
#
#         # send total request to different metric
#         if stats.get("hasMasterPrimary") == "Y":
#             self.firebaseClient.put_async(url="/totalTransactions",
#                                  name="totalTransactions",
#                                  data=stats.get('total requests'),
#                                  callback=lambda response: None,
#                                  params={'print': 'silent'},
#                                  headers={'Connection': 'keep-alive'},
#                                  )
#
#     def _sendStatsOnNodeStart(self, stats: Dict[str, object]):
#
#         self.firebaseClient.put_async(url="/startedAt", name="startedAt",
#                                  data=stats.get('startedAtData'),
#                                  callback=lambda response: None,
#                                  params={'print': 'silent'},
#                                  headers={'Connection': 'keep-alive'},
#                                  )
#
#         self.firebaseClient.put_async(url="/config", name="throughput",
#                                   data=stats.get('throughputData'),
#                                   callback=lambda response: None,
#                                   params={'print': 'silent'},
#                                   headers={'Connection': 'keep-alive'},
#                                   )
