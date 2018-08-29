from abc import ABC, abstractmethod, ABCMeta
from statistics import median_low, median, median_high
from typing import List


class AverageStrategyBase(ABC):
    @staticmethod
    def get_avg(metrics: List):
        raise NotImplementedError()


class MedianLowStrategy(AverageStrategyBase):
    @staticmethod
    def get_avg(metrics: List):
        return median_low(metrics)


class MedianMediumStrategy(AverageStrategyBase):
    @staticmethod
    def get_avg(metrics: List):
        return median(metrics)


class MedianHighStrategy(AverageStrategyBase):
    @staticmethod
    def get_avg(metrics: List):
        return median_high(metrics)


class MedianHighLatencyForAllClients(MedianHighStrategy):
    def get_latency_for_clients(self, metrics):
        return self.get_avg(metrics)


class LatencyMeasurement(metaclass=ABCMeta):
    """
    Measure latency params
    """
    @abstractmethod
    def add_duration(self, identifier, duration):
        pass

    @abstractmethod
    def get_avg_latency(self):
        pass


class EMALatencyMeasurementForEachClient(LatencyMeasurement):

    def __init__(self, config):
        self.min_latency_count = config.MIN_LATENCY_COUNT
        # map of client identifier and (total_reqs, avg_latency) tuple
        self.avg_latencies = {}    # type: Dict[str, (int, float)]
        # This parameter defines coefficient alpha, which represents the degree of weighting decrease.
        self.alpha = 1 / (self.min_latency_count + 1)
        self.total_reqs = 0
        self.avg_for_clients_cls = config.AvgStrategyForAllClients()

    def add_duration(self, identifier, duration):
        client_reqs, curr_avg_lat = self.avg_latencies.get(identifier, (0, .0))
        client_reqs += 1
        self.avg_latencies[identifier] = (client_reqs,
                                          self._accumulate(curr_avg_lat,
                                                           duration))
        self.total_reqs += 1

    def _accumulate(self, old_accum, next_val):
        """
        Implement exponential moving average
        """
        return old_accum * (1 - self.alpha) + next_val * self.alpha

    def get_avg_latency(self):
        if self.total_reqs < self.min_latency_count:
            return None
        latencies = [lat[1] for _, lat in self.avg_latencies.items()]

        return self.avg_for_clients_cls.get_latency_for_clients(latencies)


class EMALatencyMeasurementForAllClient(LatencyMeasurement):
    def __init__(self, config):
        self.min_latency_count = config.MIN_LATENCY_COUNT
        # map of client identifier and (total_reqs, avg_latency) tuple
        self.avg_latency = 0.0
        # This parameter defines coefficient alpha, which represents the degree of weighting decrease.
        self.alpha = 1 / (self.min_latency_count + 1)
        self.total_reqs = 0

    def add_duration(self, identifier, duration):
        self.avg_latency = self._accumulate(self.avg_latency, duration)
        self.total_reqs += 1

    def _accumulate(self, old_accum, next_val):
        """
        Implement exponential moving average
        """
        return old_accum * (1 - self.alpha) + next_val * self.alpha

    def get_avg_latency(self):
        if self.total_reqs < self.min_latency_count:
            return None

        return self.avg_latency
