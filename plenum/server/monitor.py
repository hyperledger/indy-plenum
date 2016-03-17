import logging
import time
from statistics import mean
from typing import Dict
from typing import List
from typing import Tuple

from plenum.common.util import getlogger
from plenum.server.instances import Instances

logger = getlogger()


class Monitor:
    """
    Implementation of RBFT's monitoring mechanism.

    The monitoring metrics are collected at the level of a node. Each node
    monitors the performance of each instance. Throughput of requests and
    latency per client request are measured.
    """

    def __init__(self, name: str, Delta: float, Lambda: float, Omega: float,
                 instances: Instances):
        self.name = name
        self.instances = instances

        self.Delta = Delta
        self.Lambda = Lambda
        self.Omega = Omega

        # Number of ordered requests by each replica. The value at index `i` in
        # the list is a tuple of the number of ordered requests by replica and
        # the time taken to order those requests by the replica of the `i`th
        # protocol instance
        self.numOrderedRequests = []  # type: List[Tuple[int, int]]

        # Requests that have been sent for ordering but have not been ordered
        # yet. Key of the dictionary is a tuple of client id and request id and
        # the value is the time at which the request was submitted for ordering
        self.requestOrderingStarted = {}  # type: Dict[Tuple[str, int], float]

        # Request latencies for the master protocol instances. Key of the
        # dictionary is a tuple of client id and request id and the value is
        # the time the master instance took for ordering it
        self.masterReqLatencies = {}  # type: Dict[Tuple[str, int], float]

        # Request latency(time taken to be ordered) for the client. The value
        # at index `i` in the list is the dictionary where the key of the
        # dictionary is the client id and the value is a tuple of number of
        # requests and average time taken by that number of requests for the
        # `i`th protocol instance
        self.clientAvgReqLatencies = []  # type: List[Dict[str, Tuple[int, float]]]

    def __repr__(self):
        return self.name

    def metrics(self):
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
            ("request ordering started", self.requestOrderingStarted),
            ("master request latencies", self.masterReqLatencies),
            ("client avg request latencies", self.clientAvgReqLatencies),
            ("throughput", {i: self.getThroughput(i)
                            for i in self.instances.ids}),
            ("master throughput", masterThrp),
            ("avg backup throughput", backupThrp),
            ("master throughput ratio", r)]
        return m

    @property
    def prettymetrics(self):
        rendered = ["{}: {}".format(*m) for m in self.metrics()]
        return "\n            ".join(rendered)

    def reset(self):
        logging.debug("Monitor being reset")
        self.numOrderedRequests = [(0, 0) for _ in self.instances.started]
        self.requestOrderingStarted = {}
        self.masterReqLatencies = {}
        self.clientAvgReqLatencies = [{} for _ in self.instances.started]

    def addInstance(self):
        """
        Add one protocol instance for monitoring.
        """
        self.instances.add()
        self.numOrderedRequests.append((0, 0))
        self.clientAvgReqLatencies.append({})

    def requestOrdered(self, identifier: str, reqId: int, instId: int,
                       byMaster: bool = False):
        """
        Measure the time taken for ordering of a request
        """
        if (identifier, reqId) not in self.requestOrderingStarted:
            logging.debug("Got ordered request with identifier {} and reqId {} "
                          "but it was from a previous view".
                          format(identifier, reqId))
            return
        duration = time.perf_counter() - self.requestOrderingStarted[
            (identifier, reqId)]
        reqs, tm = self.numOrderedRequests[instId]
        self.numOrderedRequests[instId] = (reqs + 1, tm + duration)
        if byMaster:
            self.masterReqLatencies[(identifier, reqId)] = duration
        if identifier not in self.clientAvgReqLatencies[instId]:
            self.clientAvgReqLatencies[instId][identifier] = (0, 0.0)
        totalReqs, avgTime = self.clientAvgReqLatencies[instId][identifier]
        # If avg of `n` items is `a`, thus sum of `n` items is `x` where
        # `x=n*a` then avg of `n+1` items where `y` is the new item is
        # `((n*a)+y)/n+1`
        self.clientAvgReqLatencies[instId][identifier] = \
            (totalReqs + 1, (totalReqs * avgTime + duration) / (totalReqs + 1))

    def requestUnOrdered(self, identifier: str, reqId: int):
        """
        Record the time at which request ordering started.
        """
        self.requestOrderingStarted[(identifier, reqId)] = time.perf_counter()

    def isMasterDegraded(self):
        """
        Return whether the master instance is slow.
        """
        return (self.instances.masterId is not None and
                (self.isMasterThroughputTooLow() or
                 self.isMasterReqLatencyTooHigh() or
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
        else:
            tooLow = r < self.Delta
            if tooLow:
                logger.debug("{} master throughput {} is lower than Delta {}.".
                             format(self, r, self.Delta))
            else:
                logger.trace("{} master throughput {} is acceptable.".
                             format(self, r))
            return tooLow

    def isMasterReqLatencyTooHigh(self):
        """
        Return whether the request latency of the master instance is greater
        than the acceptable threshold
        """
        r = any([lat > self.Lambda for lat
                 in self.masterReqLatencies.values()])
        if r:
            logger.debug("{} found master's latency to be higher than the "
                         "threshold for some or all requests.".format(self))
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
        logger.debug("{}'s master's avg request latency is {} and backup's "
                     "avg request latency is {} ".
                     format(self, avgLatM, avgLatB))

        # If latency of the master for any client is greater than that of
        # backups by more than the threshold `Omega`, then a view change
        # needs to happen
        for cid, lat in avgLatB.items():
            if cid not in avgLatM:
                logger.trace("{} found master had no record yet for {}".
                             format(self, cid))
                return False
            if avgLatM[cid] - lat > self.Omega:
                logger.debug("{} found difference between master's and "
                             "backups's avg latency to be higher than the "
                             "threshold".format(self))
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
        if masterThrp == 0:
            if self.numOrderedRequests[masterInstId] == (0, 0):
                avgReqsPerInst = totalReqs/self.instances.count
                if avgReqsPerInst <= 1:
                    # too early to tell if we need an instance change
                    masterThrp = None
        backupThrp = totalReqs / totalTm if totalTm else None
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
        reqs, tm = self.numOrderedRequests[instId]
        return reqs / tm if tm else None

    def getInstanceMetrics(self, forAllExcept: int) -> float:
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
        return self.mean(
            [self.clientAvgReqLatencies[i][identifier][1] for i in instId])

    def getAvgLatency(self, *instIds: int) -> Dict[str, float]:
        if len(self.clientAvgReqLatencies) == 0:
            return 0
        avgLatencies = {}
        for i in instIds:
            for cid, (numReq, avgLat) in self.clientAvgReqLatencies[i].items():
                if cid not in avgLatencies:
                    avgLatencies[cid] = []
                avgLatencies[cid].append(avgLat)

        avgLatencies = {cid: mean(lat) for cid, lat in avgLatencies.items()}

        return avgLatencies

    @staticmethod
    def mean(data):
        return 0 if len(data) == 0 else mean(data)
