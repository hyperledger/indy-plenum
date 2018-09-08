from abc import ABCMeta, abstractmethod


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
        self.avg_for_clients_cls = config.LatencyAvgStrategyForClients()

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

        return self.avg_for_clients_cls.get_avg(latencies)


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
