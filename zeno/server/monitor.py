import logging
import time
from statistics import mean
from typing import Dict
from typing import List
from typing import Tuple


class Monitor:
    """
    Implementation of RBFT's monitoring mechanism.

    The monitoring metrics are collected at the level of a node. Each node monitors the performance of each of
    replicas hosted by it. Throughput of requests and latency per client request are measured.
    """

    def __init__(self, Delta: float, Lambda: float, Omega: float):
        self.Delta = Delta
        self.Lambda = Lambda
        self.Omega = Omega

        # Started time for each replica on the node. The value at index `i` in
        # the start time of the `i`th protocol instance
        self.instanceStarted = []

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

    def reset(self):
        logging.debug("Monitor being reset")
        self.numOrderedRequests = [(0, 0) for _ in self.instanceStarted]
        self.requestOrderingStarted = {}
        self.masterReqLatencies = {}
        self.clientAvgReqLatencies = [{} for _ in self.instanceStarted]

    def addInstance(self):
        """
        Add one protocol instance for monitoring.
        """
        self.numOrderedRequests.append((0, 0))
        self.clientAvgReqLatencies.append({})
        self.instanceStarted.append(time.perf_counter())

    def requestOrdered(self, clientId: str, reqId: int, instId: int,
                       byMaster: bool = False):
        """
        Measure the time taken for ordering of a request
        """
        if (clientId, reqId) not in self.requestOrderingStarted:
            logging.debug("Got ordered request with clientId {} and reqId {} "
                          "but it was from a previous view".
                          format(clientId, reqId))
            return
        duration = time.perf_counter() - self.requestOrderingStarted[
            (clientId, reqId)]
        reqs, tm = self.numOrderedRequests[instId]
        self.numOrderedRequests[instId] = (reqs + 1, tm + duration)
        if byMaster:
            self.masterReqLatencies[(clientId, reqId)] = duration
        if clientId not in self.clientAvgReqLatencies[instId]:
            self.clientAvgReqLatencies[instId][clientId] = (0, 0.0)
        totalReqs, avgTime = self.clientAvgReqLatencies[instId][clientId]
        # If avg of `n` items is `a`, thus sum of `n` items is `x` where
        # `x=n*a` then avg of `n+1` items where `y` is the new item is
        # `((n*a)+y)/n+1`
        self.clientAvgReqLatencies[instId][clientId] = \
            (totalReqs + 1, (totalReqs * avgTime + duration) / (totalReqs + 1))

    def requestUnOrdered(self, clientId: str, reqId: int):
        """
        Record the time at which request ordering started.
        """
        self.requestOrderingStarted[(clientId, reqId)] = time.perf_counter()

    def getThroughputs(self, instId: int):
        """
        Return a tuple of  the throughput of the given instance and the average throughput of the remaining instances.

        :param instId: the id of the protocol instance
        """
        return self.getThroughput(instId), \
               self.getAvgThroughput(forAllExcept=instId)

    def getThroughput(self, instId: int) -> float:
        """
        Return the throughput of the specified instance.

        :param instId: the id of the protocol instance
        """
        # We are using the instanceStarted time in the denominator instead of
        # a time interval. This is alright for now as all the instances on a
        # node are started at almost the same time.
        if (instId >= len(self.numOrderedRequests)
            or instId >= len(self.instanceStarted)):
            return 0
        reqs, tm = self.numOrderedRequests[instId]
        return reqs / tm if tm > 0 else 0

    def getAvgThroughput(self, forAllExcept: int) -> float:
        """
        Calculate and return the average throughput of all the instances except
        the one specified as `forAllExcept`.
        """
        return self.mean([reqs / tm if tm > 0 else 0
                          for i, (reqs, tm)
                          in enumerate(self.numOrderedRequests)
                          if i != forAllExcept])

    def getAvgLatencyForClient(self, clientId: str, *instId: int) -> float:
        """
        Calculate and return the average latency of the requests of the
        client(specified by clientId) for the specified protocol instances.
        """
        if len(self.clientAvgReqLatencies) == 0:
            return 0
        return self.mean(
            [self.clientAvgReqLatencies[i][clientId][1] for i in instId])

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
