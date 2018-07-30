import time
from datetime import datetime
from statistics import mean
from typing import Dict, Iterable, Optional
from typing import List
from typing import Tuple

import psutil

from plenum.common.config_util import getConfig
from plenum.common.constants import MONITORING_PREFIX
from stp_core.common.log import getlogger
from plenum.common.types import EVENT_REQ_ORDERED, EVENT_NODE_STARTED, \
    EVENT_PERIODIC_STATS_THROUGHPUT, PLUGIN_TYPE_STATS_CONSUMER, \
    EVENT_VIEW_CHANGE, EVENT_PERIODIC_STATS_LATENCIES, \
    EVENT_PERIODIC_STATS_NODES, EVENT_PERIODIC_STATS_TOTAL_REQUESTS, \
    EVENT_PERIODIC_STATS_NODE_INFO, EVENT_PERIODIC_STATS_SYSTEM_PERFORMANCE_INFO
from plenum.server.blacklister import Blacklister
from plenum.server.has_action_queue import HasActionQueue
from plenum.server.instances import Instances
from plenum.server.notifier_plugin_manager import notifierPluginTriggerEvents, \
    PluginManager
from plenum.server.plugin.has_plugin_loader_helper import PluginLoaderHelper

pluginManager = PluginManager()
logger = getlogger()


class ThroughputMeasurement:
    """
    Measure throughput params
    """

    def __init__(self, window_size=15, min_cnt=16, first_ts=time.perf_counter()):
        self.reqs_in_window = 0
        self.throughput = 0
        self.window_size = window_size
        self.min_cnt = min_cnt
        self.first_ts = first_ts
        self.window_start_ts = self.first_ts
        self.alpha = 2 / (self.min_cnt + 1)

    def add_request(self, ordered_ts):
        self.update_time(ordered_ts)
        self.reqs_in_window += 1

    def _accumulate(self, old_accum, next_val):
        """
        Implement exponential moving average
        """
        return old_accum * (1 - self.alpha) + next_val * self.alpha

    def update_time(self, current_ts):
        while current_ts >= self.window_start_ts + self.window_size:
            self.throughput = self._accumulate(self.throughput, self.reqs_in_window / self.window_size)
            self.window_start_ts = self.window_start_ts + self.window_size
            self.reqs_in_window = 0

    def get_throughput(self, request_time):
        if request_time < self.first_ts + (self.window_size * self.min_cnt):
            return None
        self.update_time(request_time)
        return self.throughput


class LatencyMeasurement:
    """
    Measure latency params
    """

    def __init__(self, min_latency_count=10):
        self.min_latency_count = min_latency_count
        # map of client identifier and (total_reqs, avg_latency) tuple
        self.avg_latencies = {}    # type: Dict(str, (int, float))
        # This parameter defines coefficient alpha, which represents the degree of weighting decrease.
        self.alpha = 1 / (self.min_latency_count + 1)

    def add_duration(self, identifier, duration):
        total_reqs, curr_avg_lat = self.avg_latencies.get(identifier, (0, .0))
        total_reqs += 1
        self.avg_latencies[identifier] = (total_reqs,
                                          self._accumulate(curr_avg_lat,
                                                           duration))

    def _accumulate(self, old_accum, next_val):
        """
        Implement exponential moving average
        """
        return old_accum * (1 - self.alpha) + next_val * self.alpha

    def get_avg_latency(self, identifier):
        if identifier not in self.avg_latencies:
            return None
        total_reqs, curr_avg_lat = self.avg_latencies[identifier]
        if total_reqs < self.min_latency_count:
            return None

        return curr_avg_lat


class RequestTimeTracker:
    """
    Request time tracking utility
    """

    class Request:
        def __init__(self, timestamp, instance_count):
            self.timestamp = timestamp
            self.ordered = [False] * instance_count

            # True if request was unordered for too long and
            # was handled by handlers on master replica
            self.handled = False

        def order(self, instId):
            if 0 <= instId < len(self.ordered):
                self.ordered[instId] = True

        def remove_instance(self, instId):
            del self.ordered[instId]

        @property
        def is_ordered(self):
            return self.ordered[0]

        @property
        def is_handled(self):
            return self.handled

        @property
        def is_ordered_by_all(self):
            return all(self.ordered)

    def __init__(self, instance_count):
        self.instance_count = instance_count
        self._requests = {}

    def __contains__(self, item):
        return item in self._requests

    def start(self, key, timestamp):
        self._requests[key] = RequestTimeTracker.Request(timestamp, self.instance_count)

    def order(self, instId, key, timestamp):
        req = self._requests[key]
        tto = timestamp - req.timestamp
        req.order(instId)
        if req.is_ordered_by_all:
            del self._requests[key]
        return tto

    def handle(self, key):
        self._requests[key].handled = True

    def reset(self):
        self._requests.clear()

    def unordered(self):
        return ((key, req.timestamp) for key, req in self._requests.items() if not req.is_ordered)

    def handled_unordered(self):
        return ((key, req.timestamp) for key, req in self._requests.items()
                if not req.is_ordered and req.is_handled)

    def unhandled_unordered(self):
        return ((key, req.timestamp) for key, req in self._requests.items()
                if not req.is_ordered and not req.is_handled)

    def add_instance(self):
        self.instance_count += 1

    def remove_instance(self, instId):
        for req in self._requests.values():
            req.remove_instance(instId)
        reqs_to_del = [key for key, req in self._requests.items() if req.is_ordered_by_all]
        for req in reqs_to_del:
            del self._requests[req]
        self.instance_count -= 1


class Monitor(HasActionQueue, PluginLoaderHelper):
    """
    Implementation of RBFT's monitoring mechanism.

    The monitoring metrics are collected at the level of a node. Each node
    monitors the performance of each instance. Throughput of requests and
    latency per client request are measured.
    """

    def __init__(self, name: str, Delta: float, Lambda: float, Omega: float,
                 instances: Instances, nodestack,
                 blacklister: Blacklister, nodeInfo: Dict,
                 notifierEventTriggeringConfig: Dict,
                 pluginPaths: Iterable[str] = None,
                 notifierEventsEnabled: bool = True):
        self.name = name
        self.instances = instances
        self.nodestack = nodestack
        self.blacklister = blacklister
        self.nodeInfo = nodeInfo
        self.notifierEventTriggeringConfig = notifierEventTriggeringConfig
        self.notifierEventsEnabled = notifierEventsEnabled

        self.Delta = Delta
        self.Lambda = Lambda
        self.Omega = Omega
        self.statsConsumers = self.getPluginsByType(pluginPaths,
                                                    PLUGIN_TYPE_STATS_CONSUMER)

        self.config = getConfig()

        # Number of ordered requests by each replica. The value at index `i` in
        # the list is a tuple of the number of ordered requests by replica and
        # the time taken to order those requests by the replica of the `i`th
        # protocol instance
        self.numOrderedRequests = []  # type: List[Tuple[int, int]]

        # List of throughputs for replicas. Index is a instId and value is a instance of
        # ThroughputMeasurement class and provide throughputs evaluating mechanism
        self.throughputs = []   # type: List[ThroughputMeasurement]

        # Utility object for tracking requests order start and end
        # TODO: Has very similar cleanup logic to propagator.Requests
        self.requestTracker = RequestTimeTracker(instances.count)

        # Request latencies for the master protocol instances. Key of the
        # dictionary is a tuple of client id and request id and the value is
        # the time the master instance took for ordering it
        self.masterReqLatencies = {}  # type: Dict[Tuple[str, int], float]

        # Indicates that request latency in previous snapshot of master req
        # latencies was too high
        self.masterReqLatencyTooHigh = False

        # Request latency(time taken to be ordered) for the client. The value
        # at index `i` in the list is the LatencyMeasurement object which accumulate
        # average latency and total request for each client.
        self.clientAvgReqLatencies = []  # type: List[LatencyMeasurement]

        # TODO: Set this if this monitor belongs to a node which has primary
        # of master. Will be used to set `totalRequests`
        self.hasMasterPrimary = None

        # Total requests that have been ordered since the node started
        self.totalRequests = 0

        self.started = datetime.utcnow().isoformat()

        # Times of requests ordered by master in last
        # `ThroughputWindowSize` seconds. `ThroughputWindowSize` is
        # defined in config
        self.orderedRequestsInLast = []

        # Times and latencies (as a tuple) of requests ordered by master in last
        # `LatencyWindowSize` seconds. `LatencyWindowSize` is
        # defined in config
        self.latenciesByMasterInLast = []

        # Times and latencies (as a tuple) of requests ordered by backups in last
        # `LatencyWindowSize` seconds. `LatencyWindowSize` is
        # defined in config. Dictionary where key corresponds to instance id and
        #  value is a tuple of ordering time and latency of a request
        self.latenciesByBackupsInLast = {}

        # attention: handlers will work over unordered request only once
        self.unordered_requests_handlers = []  # type: List[Callable]

        # Monitoring suspicious spikes in cluster throughput
        self.clusterThroughputSpikeMonitorData = {
            'value': 0,
            'cnt': 0,
            'accum': []
        }

        psutil.cpu_percent(interval=None)
        self.lastKnownTraffic = self.calculateTraffic()

        self.totalViewChanges = 0
        self._lastPostedViewChange = 0
        HasActionQueue.__init__(self)

        if self.config.SendMonitorStats:
            self.startRepeating(self.sendPeriodicStats,
                                self.config.DashboardUpdateFreq)

        self.startRepeating(
            self.checkPerformance,
            self.config.notifierEventTriggeringConfig['clusterThroughputSpike']['freq'])

        self.startRepeating(self.check_unordered, self.config.UnorderedCheckFreq)

        if 'disable_view_change' in self.config.unsafe:
            self.isMasterDegraded = lambda: False
        if 'disable_monitor' in self.config.unsafe:
            self.requestOrdered = lambda *args, **kwargs: {}
            self.sendPeriodicStats = lambda: None
            self.checkPerformance = lambda: None

    def __repr__(self):
        return self.name

    def metrics(self):
        """
        Calculate and return the metrics.
        """
        masterThrp, backupThrp = self.getThroughputs(self.instances.masterId)
        r = self.masterThroughputRatio()
        m = [
            ("{} Monitor metrics:".format(self), None),
            ("Delta", self.Delta),
            ("Lambda", self.Lambda),
            ("Omega", self.Omega),
            ("instances started", self.instances.started),
            ("ordered request counts",
             {i: r[0] for i, r in enumerate(self.numOrderedRequests)}),
            ("ordered request durations",
             {i: r[1] for i, r in enumerate(self.numOrderedRequests)}),
            ("master request latencies", self.masterReqLatencies),
            ("client avg request latencies", {i: self.getAvgLatency(i)
                                              for i in self.instances.ids}),
            ("throughput", {i: self.getThroughput(i)
                            for i in self.instances.ids}),
            ("master throughput", masterThrp),
            ("total requests", self.totalRequests),
            ("avg backup throughput", backupThrp),
            ("master throughput ratio", r)]
        return m

    @property
    def prettymetrics(self) -> str:
        """
        Pretty printing for metrics
        """
        rendered = ["{}: {}".format(*m) for m in self.metrics()]
        return "\n            ".join(rendered)

    def calculateTraffic(self):
        currNetwork = psutil.net_io_counters()
        currNetwork = currNetwork.bytes_sent + currNetwork.bytes_recv
        currNetwork /= 1024
        return currNetwork

    def reset(self):
        """
        Reset the monitor. Sets all monitored values to defaults.
        """
        logger.debug("{}'s Monitor being reset".format(self))
        num_instances = len(self.instances.started)
        self.numOrderedRequests = [(0, 0)] * num_instances
        self.requestTracker.reset()
        self.masterReqLatencies = {}
        self.masterReqLatencyTooHigh = False
        self.totalViewChanges += 1
        self.lastKnownTraffic = self.calculateTraffic()
        for i in range(num_instances):
            rm = ThroughputMeasurement(window_size=self.config.ThroughputInnerWindowSize,
                                       min_cnt=self.config.ThroughputMinActivityThreshold,
                                       first_ts=time.perf_counter())
            self.throughputs[i] = rm
            lm = LatencyMeasurement(min_latency_count=self.config.MIN_LATENCY_COUNT)
            self.clientAvgReqLatencies[i] = lm

    def addInstance(self):
        """
        Add one protocol instance for monitoring.
        """
        self.instances.add()
        self.requestTracker.add_instance()
        self.numOrderedRequests.append((0, 0))
        rm = ThroughputMeasurement(window_size=self.config.ThroughputInnerWindowSize,
                                   min_cnt=self.config.ThroughputMinActivityThreshold,
                                   first_ts=time.perf_counter())

        self.throughputs.append(rm)
        lm = LatencyMeasurement(min_latency_count=self.config.MIN_LATENCY_COUNT)
        self.clientAvgReqLatencies.append(lm)

    def removeInstance(self, index=None):
        if self.instances.count > 0:
            if index is None:
                index = self.instances.count - 1
            self.instances.remove(index)
            self.requestTracker.remove_instance(index)
            del self.numOrderedRequests[index]
            del self.clientAvgReqLatencies[index]
            del self.throughputs[index]

    def requestOrdered(self, reqIdrs: List[Tuple[str, int]], instId: int,
                       requests, byMaster: bool = False) -> Dict:
        """
        Measure the time taken for ordering of a request and return it. Monitor
        might have been reset due to view change due to which this method
        returns None
        """
        now = time.perf_counter()
        durations = {}
        for key in reqIdrs:
            if key not in self.requestTracker:
                logger.debug("Got untracked ordered request with digest {}".
                             format(key))
                continue
            for reqId, started in self.requestTracker.handled_unordered():
                if reqId == key:
                    logger.info('Consensus for ReqId: {} was achieved by {}:{} in {} seconds.'
                                .format(reqId, self.name, instId, now - started))
                    continue
            duration = self.requestTracker.order(instId, key, now)
            self.throughputs[instId].add_request(now)
            if byMaster:
                # TODO for now, view_change procedure can take more that 15 minutes
                # (5 minutes for catchup and 10 minutes for primary's answer).
                # Therefore, view_change triggering by max latency is not indicative now.
                # self.masterReqLatencies[key] = duration
                self.orderedRequestsInLast.append(now)
                self.latenciesByMasterInLast.append((now, duration))
            else:
                self.latenciesByBackupsInLast.setdefault(instId, []).append((now, duration))

            if key in requests:
                identifier = requests[key].request.identifier
                self.clientAvgReqLatencies[instId].add_duration(identifier, duration)

            durations[key] = duration

        reqs, tm = self.numOrderedRequests[instId]
        orderedNow = len(durations)
        self.numOrderedRequests[instId] = (reqs + orderedNow,
                                           tm + sum(durations.values()))

        # TODO: Inefficient, as on every request a minimum of a large list is
        # calculated
        if min(r[0] for r in self.numOrderedRequests) == (reqs + orderedNow):
            # If these requests is ordered by the last instance then increment
            # total requests, but why is this important, why cant is ordering
            # by master not enough?
            self.totalRequests += orderedNow
            self.postOnReqOrdered()
            if 0 == reqs:
                self.postOnNodeStarted(self.started)

        return durations

    def requestUnOrdered(self, key: str):
        """
        Record the time at which request ordering started.
        """
        self.requestTracker.start(key, time.perf_counter())

    def check_unordered(self):
        now = time.perf_counter()
        new_unordereds = [(req, now - started) for req, started in self.requestTracker.unhandled_unordered()
                          if now - started > self.config.UnorderedCheckFreq]
        if len(new_unordereds) == 0:
            return
        for handler in self.unordered_requests_handlers:
            handler(new_unordereds)
        for unordered in new_unordereds:
            self.requestTracker.handle(unordered[0])
            logger.debug('Following requests were not ordered for more than {} seconds: {}'
                         .format(self.config.UnorderedCheckFreq, unordered[0]))

    def isMasterDegraded(self):
        """
        Return whether the master instance is slow.
        """
        return (self.instances.masterId is not None and
                (self.isMasterThroughputTooLow() or
                 # TODO for now, view_change procedure can take more that 15 minutes
                 # (5 minutes for catchup and 10 minutes for primary's answer).
                 # Therefore, view_change triggering by max latency now is not indicative.
                 # self.isMasterReqLatencyTooHigh() or
                 self.isMasterAvgReqLatencyTooHigh()))

    def masterThroughputRatio(self):
        """
        The relative throughput of the master instance compared to the backup
        instances.
        """
        masterThrp, backupThrp = self.getThroughputs(self.instances.masterId)

        # Backup throughput may be 0 so moving ahead only if it is not 0
        r = masterThrp / backupThrp if backupThrp and masterThrp is not None \
            else None
        return r

    def isMasterThroughputTooLow(self):
        """
        Return whether the throughput of the master instance is greater than the
        acceptable threshold
        """
        r = self.masterThroughputRatio()
        if r is None:
            logger.debug("{} master throughput is not measurable.".
                         format(self))
            return None

        tooLow = r < self.Delta
        if tooLow:
            logger.display("{}{} master throughput ratio {} is lower than Delta {}.".
                           format(MONITORING_PREFIX, self, r, self.Delta))
        else:
            logger.trace("{} master throughput ratio {} is acceptable.".
                         format(self, r))
        return tooLow

    def isMasterReqLatencyTooHigh(self):
        """
        Return whether the request latency of the master instance is greater
        than the acceptable threshold
        """
        # TODO for now, view_change procedure can take more that 15 minutes
        # (5 minutes for catchup and 10 minutes for primary's answer).
        # Therefore, view_change triggering by max latency is not indicative now.

        r = self.masterReqLatencyTooHigh or \
            next(((key, lat) for key, lat in self.masterReqLatencies.items() if
                  lat > self.Lambda), None)
        if r:
            logger.display("{}{} found master's latency {} to be higher than the threshold for request {}.".
                           format(MONITORING_PREFIX, self, r[1], r[0]))
        else:
            logger.trace("{} found master's latency to be lower than the "
                         "threshold for all requests.".format(self))
        return r

    def isMasterAvgReqLatencyTooHigh(self):
        """
        Return whether the average request latency of the master instance is
        greater than the acceptable threshold
        """
        avgLatM = self.getAvgLatency(self.instances.masterId)
        avgLatB = self.getAvgLatency(*self.instances.backupIds)

        # If latency of the master for any client is greater than that of
        # backups by more than the threshold `Omega`, then a view change
        # needs to happen
        for cid, lat in avgLatB.items():
            if cid not in avgLatM:
                logger.trace("{} found master had no record yet for {}".
                             format(self, cid))
                return False
            d = avgLatM[cid] - lat
            if d > self.Omega:
                logger.info("{}{} found difference between master's and "
                            "backups's avg latency {} to be higher than the "
                            "threshold".format(MONITORING_PREFIX, self, d))
                logger.trace(
                    "{}'s master's avg request latency is {} and backup's "
                    "avg request latency is {}".format(self, avgLatM, avgLatB))
                return True
        logger.trace("{} found difference between master and backups "
                     "avg latencies to be acceptable".format(self))
        return False

    def getThroughputs(self, masterInstId: int):
        """
        Return a tuple of  the throughput of the given instance and the average
        throughput of the remaining instances.

        :param instId: the id of the protocol instance
        """

        masterThrp = self.getThroughput(masterInstId)
        totalReqs, totalTm = self.getInstanceMetrics(forAllExcept=masterInstId)
        # Average backup replica's throughput
        if len(self.throughputs) > 1:
            perf_time = time.perf_counter()
            thrs = []
            for instId, thr_obj in enumerate(self.throughputs):
                if instId != masterInstId:
                    thr = thr_obj.get_throughput(perf_time)
                    if thr is not None:
                        thrs.append(thr)
            backupThrp = sum(thrs) / len(thrs) if thrs else None

        else:
            backupThrp = None
        if masterThrp == 0:
            if self.numOrderedRequests[masterInstId] == (0, 0):
                avgReqsPerInst = (totalReqs or 0) / self.instances.count
                if avgReqsPerInst <= 1:
                    # too early to tell if we need an instance change
                    masterThrp = None
        return masterThrp, backupThrp

    def getThroughput(self, instId: int) -> float:
        """
        Return the throughput of the specified instance.

        :param instId: the id of the protocol instance
        """
        # We are using the instanceStarted time in the denominator instead of
        # a time interval. This is alright for now as all the instances on a
        # node are started at almost the same time.
        if instId >= self.instances.count:
            return None
        perf_time = time.perf_counter()
        throughput = self.throughputs[instId].get_throughput(perf_time)
        return throughput

    def getInstanceMetrics(
            self, forAllExcept: int) -> Tuple[Optional[int], Optional[float]]:
        """
        Calculate and return the average throughput of all the instances except
        the one specified as `forAllExcept`.
        """
        m = [(reqs, tm) for i, (reqs, tm)
             in enumerate(self.numOrderedRequests)
             if i != forAllExcept]
        if m:
            reqs, tm = zip(*m)
            return sum(reqs), sum(tm)
        else:
            return None, None

    def getAvgLatencyForClient(self, identifier: str, *instId: int) -> float:
        """
        Calculate and return the average latency of the requests of the
        client(specified by identifier) for the specified protocol instances.
        """
        if len(self.clientAvgReqLatencies) == 0:
            return 0
        means = []
        for i in instId:
            avg_lat = self.clientAvgReqLatencies[i].get_avg_latency(identifier)
            if avg_lat:
                means.append(avg_lat)
        return self.mean(means)

    def getAvgLatency(self, *instIds: int) -> Dict[str, float]:
        if len(self.clientAvgReqLatencies) == 0:
            return 0
        avgLatencies = {}
        for i in instIds:
            for cid in self.clientAvgReqLatencies[i].avg_latencies.keys():
                avg_lat = self.clientAvgReqLatencies[i].get_avg_latency(cid)
                if avg_lat:
                    avgLatencies.setdefault(cid, []).append(avg_lat)

        avgLatencies = {cid: mean(lat) for cid, lat in avgLatencies.items()}

        return avgLatencies

    def sendPeriodicStats(self):
        thoughputData = self.sendThroughput()
        self.clusterThroughputSpikeMonitorData['accum'].append(
            thoughputData['throughput'])
        self.sendLatencies()
        self.sendKnownNodesInfo()
        self.sendNodeInfo()
        self.sendSystemPerfomanceInfo()
        self.sendTotalRequests()

    def checkPerformance(self):
        self.sendClusterThroughputSpike()

    def sendClusterThroughputSpike(self):
        if self.instances.masterId is None:
            return None
        accum = 0
        for val in self.clusterThroughputSpikeMonitorData['accum']:
            accum += val
        if len(self.clusterThroughputSpikeMonitorData['accum']):
            accum /= len(self.clusterThroughputSpikeMonitorData['accum'])
        self.clusterThroughputSpikeMonitorData['accum'] = []
        return pluginManager.sendMessageUponSuspiciousSpike(
            notifierPluginTriggerEvents['clusterThroughputSpike'],
            self.clusterThroughputSpikeMonitorData,
            accum,
            self.notifierEventTriggeringConfig['clusterThroughputSpike'],
            self.name,
            self.notifierEventsEnabled
        )

    @property
    def highResThroughput(self):
        # TODO:KS Move these computations as well to plenum-stats project
        now = time.perf_counter()
        while self.orderedRequestsInLast and \
                (now - self.orderedRequestsInLast[0]) > \
                self.config.ThroughputWindowSize:
            self.orderedRequestsInLast = self.orderedRequestsInLast[1:]

        return len(self.orderedRequestsInLast) / self.config.ThroughputWindowSize

    def sendThroughput(self):
        logger.debug("{} sending throughput".format(self))

        throughput = self.highResThroughput
        utcTime = datetime.utcnow()
        mtrStats = {
            "throughput": throughput,
            "timestamp": utcTime.isoformat(),
            "nodeName": self.name,
            # Multiply by 1000 for JavaScript date conversion
            "time": time.mktime(utcTime.timetuple()) * 1000
        }
        self._sendStatsDataIfRequired(
            EVENT_PERIODIC_STATS_THROUGHPUT, mtrStats)
        return mtrStats

    @property
    def masterLatency(self):
        now = time.perf_counter()
        while self.latenciesByMasterInLast and \
                (now - self.latenciesByMasterInLast[0][0]) > \
                self.config.LatencyWindowSize:
            self.latenciesByMasterInLast = self.latenciesByMasterInLast[1:]
        return (sum(l[1] for l in self.latenciesByMasterInLast) /
                len(self.latenciesByMasterInLast)) if \
            len(self.latenciesByMasterInLast) > 0 else 0

    @property
    def avgBackupLatency(self):
        now = time.perf_counter()
        backupLatencies = []
        for instId, latencies in self.latenciesByBackupsInLast.items():
            while latencies and \
                    (now - latencies[0][0]) > \
                    self.config.LatencyWindowSize:
                latencies = latencies[1:]
            backupLatencies.append(
                (sum(l[1] for l in latencies) / len(latencies)) if
                len(latencies) > 0 else 0)
            self.latenciesByBackupsInLast[instId] = latencies

        return self.mean(backupLatencies)

    def sendLatencies(self):
        logger.debug("{} sending latencies".format(self))
        utcTime = datetime.utcnow()
        # Multiply by 1000 to make it compatible to JavaScript Date()
        jsTime = time.mktime(utcTime.timetuple()) * 1000

        latencies = dict(
            masterLatency=self.masterLatency,
            averageBackupLatency=self.avgBackupLatency,
            time=jsTime,
            nodeName=self.name,
            timestamp=utcTime.isoformat()
        )

        self._sendStatsDataIfRequired(
            EVENT_PERIODIC_STATS_LATENCIES, latencies)

    def sendKnownNodesInfo(self):
        logger.debug("{} sending nodestack".format(self))
        self._sendStatsDataIfRequired(
            EVENT_PERIODIC_STATS_NODES, remotesInfo(
                self.nodestack, self.blacklister))

    def sendSystemPerfomanceInfo(self):
        logger.debug("{} sending system performance".format(self))
        self._sendStatsDataIfRequired(
            EVENT_PERIODIC_STATS_SYSTEM_PERFORMANCE_INFO,
            self.captureSystemPerformance())

    def sendNodeInfo(self):
        logger.debug("{} sending node info".format(self))
        self._sendStatsDataIfRequired(
            EVENT_PERIODIC_STATS_NODE_INFO, self.nodeInfo['data'])

    def sendTotalRequests(self):
        logger.debug("{} sending total requests".format(self))

        totalRequests = dict(
            totalRequests=self.totalRequests
        )

        self._sendStatsDataIfRequired(
            EVENT_PERIODIC_STATS_TOTAL_REQUESTS, totalRequests)

    def captureSystemPerformance(self):
        logger.debug("{} capturing system performance".format(self))
        timestamp = time.time()
        cpu = psutil.cpu_percent(interval=None)
        ram = psutil.virtual_memory()
        curr_network = self.calculateTraffic()
        network = curr_network - self.lastKnownTraffic
        self.lastKnownTraffic = curr_network
        cpu_data = {
            'time': timestamp,
            'value': cpu
        }
        ram_data = {
            'time': timestamp,
            'value': ram.percent
        }
        traffic_data = {
            'time': timestamp,
            'value': network
        }
        return {
            'cpu': cpu_data,
            'ram': ram_data,
            'traffic': traffic_data
        }

    def postOnReqOrdered(self):
        utcTime = datetime.utcnow()
        # Multiply by 1000 to make it compatible to JavaScript Date()
        jsTime = time.mktime(utcTime.timetuple()) * 1000

        if self.totalViewChanges != self._lastPostedViewChange:
            self._lastPostedViewChange = self.totalViewChanges
            viewChange = dict(
                time=jsTime,
                viewChange=self._lastPostedViewChange
            )
            self._sendStatsDataIfRequired(EVENT_VIEW_CHANGE, viewChange)

        reqOrderedEventDict = dict(self.metrics())
        reqOrderedEventDict["created_at"] = utcTime.isoformat()
        reqOrderedEventDict["nodeName"] = self.name
        reqOrderedEventDict["time"] = jsTime
        reqOrderedEventDict["hasMasterPrimary"] = "Y" if self.hasMasterPrimary else "N"
        self._sendStatsDataIfRequired(EVENT_REQ_ORDERED, reqOrderedEventDict)
        self._clearSnapshot()

    def postOnNodeStarted(self, startedAt):
        throughputData = {
            "throughputWindowSize": self.config.ThroughputWindowSize,
            "updateFrequency": self.config.DashboardUpdateFreq,
            "graphDuration": self.config.ThroughputGraphDuration
        }
        startedAtData = {"startedAt": startedAt, "ctx": "DEMO"}
        startedEventDict = {
            "startedAtData": startedAtData,
            "throughputConfig": throughputData
        }
        self._sendStatsDataIfRequired(EVENT_NODE_STARTED, startedEventDict)

    def _clearSnapshot(self):
        self.masterReqLatencyTooHigh = self.isMasterReqLatencyTooHigh()
        self.masterReqLatencies = {}

    def _sendStatsDataIfRequired(self, event, stats):
        if self.config.SendMonitorStats:
            for sc in self.statsConsumers:
                sc.sendStats(event, stats)

    @staticmethod
    def mean(data):
        return 0 if len(data) == 0 else mean(data)


def remotesInfo(nodestack, blacklister):
    res = {
        'connected': [],
        'disconnected': []
    }

    conns, disconns = nodestack.remotesByConnected()

    for r in conns:
        res['connected'].append(remoteInfo(r, nodestack, blacklister))
    for r in disconns:
        res['disconnected'].append(remoteInfo(r, nodestack, blacklister))

    return res


def remoteInfo(remote, nodestack, blacklister):
    regName = nodestack.findInNodeRegByHA(remote.ha)
    res = pickRemoteEstateFields(remote, regName)
    res['blacklisted'] = blacklister.isBlacklisted(remote.name)
    if not res['blacklisted'] and regName:
        res['blacklisted'] = blacklister.isBlacklisted(regName)
    return res


def pickRemoteEstateFields(remote, customName=None):
    host, port = remote.ha
    return {
        'name': customName or remote.name,
        'host': host,
        'port': port,
        'nat': getattr(remote, 'natted', False) or False
    }
