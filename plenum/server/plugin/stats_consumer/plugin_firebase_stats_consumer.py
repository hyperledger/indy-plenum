import importlib
from abc import abstractmethod
from datetime import datetime
from functools import partial
from typing import Dict

import jsonpickle

from plenum.common.types import EVENT_PERIODIC_STATS_THROUGHPUT, EVENT_NODE_STARTED, EVENT_REQ_ORDERED, \
    PLUGIN_TYPE_STATS_CONSUMER
from plenum.common.util import getlogger
from plenum.server.plugin_loader import HasDynamicallyImportedModules
from plenum.server.stats_consumer import StatsConsumer

logger = getlogger()
firebaseModule = None
firebaseModuleImportedSuccessfully = False

try:
    firebaseModule = importlib.import_module("firebase")
    firebaseModule.process_pool.terminate()
    firebaseModule.async._process_pool = None
    # Temporary fix for letting firebase create only 1 extra process
    # TODO: This needs to be some kind of configuration option
    firebaseModule.process_pool = firebaseModule.lazy.LazyLoadProxy(partial(firebaseModule.async.__dict__.get('get_process_pool'), 1))
    firebasePkg = importlib.import_module("firebase.firebase")
    firebaseModuleImportedSuccessfully = True
except ImportError:
    firebaseModuleImportedSuccessfully = False
    logger.warning("** NOTE: seems firebase dependency NOT installed, "
                   "please install it if you want to send stats to firebase")


class FirebaseStatsConsumer(StatsConsumer, HasDynamicallyImportedModules):
    pluginType = PLUGIN_TYPE_STATS_CONSUMER

    def __init__(self):
        super().__init__()
        self._firebaseClient = None
        self._firebaseModuleImportedSuccessfully = firebaseModuleImportedSuccessfully
        self._eventToFunc = {
            EVENT_REQ_ORDERED: self._sendStatsOnReqOrdered,
            EVENT_NODE_STARTED: self._sendStatsOnNodeStart,
            EVENT_PERIODIC_STATS_THROUGHPUT: self._periodicStatsThroughput,
        }

    @abstractmethod
    def isModuleImportedSuccessfully(self):
        return self._firebaseModuleImportedSuccessfully

    @property
    def firebaseClient(self):
        if self._firebaseClient:
            return self._firebaseClient
        else:
            self._firebaseClient = firebasePkg.FirebaseApplication(
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