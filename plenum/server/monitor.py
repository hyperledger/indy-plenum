import time
from datetime import datetime
from statistics import mean
from typing import Dict, Iterable, Optional
from typing import List
from typing import Tuple

import psutil

from plenum.common.config_util import getConfig
from plenum.common.constants import MONITORING_PREFIX
from plenum.common.monitor_strategies import AccumulatingMonitorStrategy
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


class RequestTimeTracker:
    """
    Request time tracking utility
    """

    class Request:
        def __init__(self, timestamp, instances_ids):
            self.ordered = dict()
            self.timestamp = timestamp
            for ins_id in instances_ids:
                self.ordered[ins_id] = False

            # True if request was unordered for too long and
            # was handled by handlers on master replica
            self.handled = False

        def order(self, instId):
            if instId in self.ordered:
                self.ordered[instId] = True

        def remove_instance(self, instId):
            self.ordered.pop(instId, None)

        @property
        def is_ordered(self):
            return self.ordered[0]

        @property
        def is_handled(self):
            return self.handled

        @property
        def is_ordered_by_all(self):
            return all(self.ordered.values())

    def __init__(self, instances_ids):
        self.instances_ids = instances_ids
        self._requests = {}
        self._unordered = set()
        self._handled_unordered = set()

    def __len__(self):
        return len(self._requests)

    def __contains__(self, item):
        return item in self._requests

    def started(self, key):
        req = self._requests.get(key)
        return req.timestamp if req is not None else None

    def start(self, key, timestamp):
        self._requests[key] = RequestTimeTracker.Request(timestamp, self.instances_ids)
        self._unordered.add(key)

    def order(self, instId, key, timestamp):
        req = self._requests[key]
        tto = timestamp - req.timestamp
        req.order(instId)
        if instId == 0:
            self._handled_unordered.discard(key)
            self._unordered.discard(key)
        if req.is_ordered_by_all:
            del self._requests[key]
        return tto

    def handle(self, key):
        self._requests[key].handled = True
        self._handled_unordered.add(key)

    def reset(self):
        self._requests.clear()
        self._unordered.clear()
        self._handled_unordered.clear()

    def unordered(self):
        return self._unordered

    def handled_unordered(self):
        return self._handled_unordered

    def unhandled_unordered(self):
        return ((key, req.timestamp) for key, req in self._requests.items()
                if not req.is_ordered and not req.is_handled)

    def add_instance(self, inst_id):
        self.instances_ids.add(inst_id)

    def remove_instance(self, instId):
        for req in self._requests.values():
            req.remove_instance(instId)
        reqs_to_del = [key for key, req in self._requests.items() if req.is_ordered_by_all]
        for req in reqs_to_del:
            del self._requests[req]
            self._unordered.discard(req)
            self._handled_unordered.discard(req)
        self.instances_ids.remove(instId)


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

        # Number of ordered requests by each replica. The value at key `i` in
        # the dict is a tuple of the number of ordered requests by replica and
        # the time taken to order those requests by the replica of the `i`th
        # protocol instance
        self.numOrderedRequests = dict()  # type: Dict[int, Tuple[int, int]]

        # Dict(instance_id, throughput) of throughputs for replicas. Key is a instId and value is a instance of
        # ThroughputMeasurement class and provide throughputs evaluating mechanism
        self.throughputs = dict()   # type: Dict[int, ThroughputMeasurement]

        # Utility object for tracking requests order start and end
        # TODO: Has very similar cleanup logic to propagator.Requests
        self.requestTracker = RequestTimeTracker(instances.ids)

        # Request latencies for the master protocol instances. Key of the
        # dictionary is a tuple of client id and request id and the value is
        # the time the master instance took for ordering it
        self.masterReqLatencies = {}  # type: Dict[Tuple[str, int], float]

        # Indicates that request latency in previous snapshot of master req
        # latencies was too high
        self.masterReqLatencyTooHigh = False

        # Request latency(time taken to be ordered) for the client. The value
        # at key `i` in the dict is the LatencyMeasurement object which accumulate
        # average latency and total request for each client.
        self.clientAvgReqLatencies = dict()  # type: Dict[int, LatencyMeasurement]

        # TODO: Set this if this monitor belongs to a node which has primary
        # of master. Will be used to set `totalRequests`
        self.hasMasterPrimary = None

        # Total requests that have been ordered since the node started
        self.totalRequests = 0

        self.started = datetime.utcnow().isoformat()

        self.orderedRequestsInLast = []

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

        self.latency_avg_for_backup_cls = self.config.LatencyAveragingStrategyClass
        self.latency_measurement_cls = self.config.LatencyMeasurementCls
        self.throughput_avg_strategy_cls = self.config.throughput_averaging_strategy_class

        self.acc_monitor = None

        if self.config.ACC_MONITOR_ENABLED:
            self.acc_monitor = AccumulatingMonitorStrategy(
                start_time=time.perf_counter(),
                instances=instances.ids,
                txn_delta_k=self.config.ACC_MONITOR_TXN_DELTA_K,
                timeout=self.config.ACC_MONITOR_TIMEOUT,
                input_rate_reaction_half_time=self.config.ACC_MONITOR_INPUT_RATE_REACTION_HALF_TIME)

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
             {i: r[0] for i, r in self.numOrderedRequests.items()}),
            ("ordered request durations",
             {i: r[1] for i, r in self.numOrderedRequests.items()}),
            ("master request latencies", self.masterReqLatencies),
            ("client avg request latencies", {i: self.getLatency(i)
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

    @staticmethod
    def create_throughput_measurement(config, start_ts=time.perf_counter()):
        tm = config.throughput_measurement_class(
            **config.throughput_measurement_params)
        tm.init_time(start_ts)
        return tm

    def reset(self):
        """
        Reset the monitor. Sets all monitored values to defaults.
        """
        logger.debug("{}'s Monitor being reset".format(self))
        instances_ids = self.instances.started.keys()
        self.numOrderedRequests = {inst_id: (0, 0) for inst_id in instances_ids}
        self.requestTracker.reset()
        self.masterReqLatencies = {}
        self.masterReqLatencyTooHigh = False
        self.totalViewChanges += 1
        self.lastKnownTraffic = self.calculateTraffic()
        if self.acc_monitor:
            self.acc_monitor.reset()
        for i in instances_ids:
            rm = self.create_throughput_measurement(self.config)
            self.throughputs[i] = rm
            lm = self.latency_measurement_cls(self.config)
            self.clientAvgReqLatencies[i] = lm

    def addInstance(self, inst_id):
        """
        Add one protocol instance for monitoring.
        """
        self.instances.add(inst_id)
        self.requestTracker.add_instance(inst_id)
        self.numOrderedRequests[inst_id] = (0, 0)
        rm = self.create_throughput_measurement(self.config)

        self.throughputs[inst_id] = rm
        lm = self.latency_measurement_cls(self.config)
        self.clientAvgReqLatencies[inst_id] = lm
        if self.acc_monitor:
            self.acc_monitor.add_instance(inst_id)

    def removeInstance(self, inst_id):
        if self.acc_monitor:
            self.acc_monitor.remove_instance(inst_id)
        if self.instances.count > 0:
            self.instances.remove(inst_id)
            self.requestTracker.remove_instance(inst_id)
            self.numOrderedRequests.pop(inst_id, None)
            self.clientAvgReqLatencies.pop(inst_id, None)
            self.throughputs.pop(inst_id, None)

    def requestOrdered(self, reqIdrs: List[str], instId: int,
                       requests, byMaster: bool = False) -> Dict:
        """
        Measure the time taken for ordering of a request and return it. Monitor
        might have been reset due to view change due to which this method
        returns None
        """
        now = time.perf_counter()
        if self.acc_monitor:
            self.acc_monitor.update_time(now)
        durations = {}
        for key in reqIdrs:
            if key not in self.requestTracker:
                logger.debug("Got untracked ordered request with digest {}".
                             format(key))
                continue
            if self.acc_monitor:
                self.acc_monitor.request_ordered(key, instId)
            if key in self.requestTracker.handled_unordered():
                started = self.requestTracker.started(key)
                logger.info('Consensus for ReqId: {} was achieved by {}:{} in {} seconds.'
                            .format(key, self.name, instId, now - started))
                continue
            duration = self.requestTracker.order(instId, key, now)
            self.throughputs[instId].add_request(now)
            if byMaster:
                # TODO for now, view_change procedure can take more that 15 minutes
                # (5 minutes for catchup and 10 minutes for primary's answer).
                # Therefore, view_change triggering by max latency is not indicative now.
                # self.masterReqLatencies[key] = duration
                self.orderedRequestsInLast.append(now)

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
        if min(r[0] for r in self.numOrderedRequests.values()) == (reqs + orderedNow):
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
        now = time.perf_counter()
        if self.acc_monitor:
            self.acc_monitor.update_time(now)
            self.acc_monitor.request_received(key)
        self.requestTracker.start(key, now)

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
        if self.acc_monitor:
            self.acc_monitor.update_time(time.perf_counter())
            return self.acc_monitor.is_master_degraded()
        else:
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
        avg_lat_master, avg_lat_backup = self.getLatencies()
        if not avg_lat_master or not avg_lat_backup:
            return False

        d = avg_lat_master - avg_lat_backup
        if d < self.Omega:
            return False

        logger.info("{}{} found difference between master's and "
                    "backups's avg latency {} to be higher than the "
                    "threshold".format(MONITORING_PREFIX, self, d))
        logger.trace(
            "{}'s master's avg request latency is {} and backup's "
            "avg request latency is {}".format(self, avg_lat_master, avg_lat_backup))
        return True

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
            thrs = []
            for instId, thr_obj in self.throughputs.items():
                if instId != masterInstId:
                    thr = self.getThroughput(instId)
                    if thr is not None:
                        thrs.append(thr)
            if thrs:
                backupThrp = self.throughput_avg_strategy_cls.get_avg(thrs)
            else:
                backupThrp = None
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
        if instId not in self.instances.ids:
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
             in self.numOrderedRequests.items()
             if i != forAllExcept]
        if m:
            reqs, tm = zip(*m)
            return sum(reqs), sum(tm)
        else:
            return None, None

    def getLatencies(self):
        avg_lat_master = self.getLatency(self.instances.masterId)
        avg_lat_backup_by_inst = []
        for instId in self.instances.backupIds:
            lat = self.getLatency(instId)
            if lat:
                avg_lat_backup_by_inst.append(lat)
        avg_lat_backup_ = self.latency_avg_for_backup_cls.get_avg(avg_lat_backup_by_inst)\
            if avg_lat_backup_by_inst else None
        return avg_lat_master, avg_lat_backup_

    def getLatency(self, instId: int) -> float:
        """
        Return a dict with client identifier as a key and calculated latency as a value
        """
        if len(self.clientAvgReqLatencies) == 0:
            return 0.0
        return self.clientAvgReqLatencies[instId].get_avg_latency()

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
        return self.getThroughput(self.instances.masterId)

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
        master_latency, _ = self.getLatencies()
        return master_latency

    @property
    def avgBackupLatency(self):
        _, lat_backup = self.getLatencies()
        return lat_backup

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
