from abc import ABC, abstractmethod
from collections import defaultdict

from plenum.common.moving_average import EMAEventFrequencyEstimator
from stp_core.common.log import getlogger

logger = getlogger()


class MonitorStrategy(ABC):
    @abstractmethod
    def add_instance(self, inst_id):
        pass

    @abstractmethod
    def remove_instance(self, inst_id):
        pass

    @abstractmethod
    def reset(self):
        pass

    @abstractmethod
    def update_time(self, timestamp: float):
        pass

    @abstractmethod
    def request_received(self, id: str):
        pass

    @abstractmethod
    def request_ordered(self, id: str, inst_id: int):
        pass

    @abstractmethod
    def is_master_degraded(self) -> bool:
        return False

    @abstractmethod
    def is_instance_degraded(self, inst_id: int) -> bool:
        return False


class AccumulatingMonitorStrategy(MonitorStrategy):
    def __init__(self, start_time: float, instances: set, txn_delta_k: int, timeout: float,
                 input_rate_reaction_half_time: float):
        self._instances = instances
        self._txn_delta_k = txn_delta_k
        self._timeout = timeout
        self._ordered = defaultdict(int)
        self._timestamp = start_time
        self._alert_timestamp = defaultdict(lambda: None)
        self._input_txn_rate = EMAEventFrequencyEstimator(start_time, input_rate_reaction_half_time)

    def add_instance(self, inst_id):
        self._instances.add(inst_id)

    def remove_instance(self, inst_id):
        self._instances.remove(inst_id)

    def reset(self):
        logger.info("Resetting accumulating monitor")
        self._input_txn_rate.reset(self._timestamp)
        self._alert_timestamp = {inst: None for inst in self._instances}
        self._ordered.clear()

    def update_time(self, timestamp: float):
        self._timestamp = timestamp
        self._input_txn_rate.update_time(timestamp)
        for inst_id in self._instances:
            is_alerted = self._alert_timestamp[inst_id] is not None
            is_degraded = self._is_degraded(inst_id)
            if is_alerted and not is_degraded:
                logger.info("Accumulating monitor is no longer alerted on instance {}, {}".
                            format(inst_id, self._statistics()))
                self._alert_timestamp[inst_id] = None
            elif not is_alerted and is_degraded:
                logger.info("Accumulating monitor became alerted on instance {}, {}".
                            format(inst_id, self._statistics()))
                self._alert_timestamp[inst_id] = self._timestamp

    def request_received(self, id: str):
        self._input_txn_rate.add_events(1)

    def request_ordered(self, id: str, inst_id: int):
        self._ordered[inst_id] += 1

    def is_master_degraded(self) -> bool:
        return self.is_instance_degraded(0)

    def is_instance_degraded(self, inst_id: int) -> bool:
        if self._alert_timestamp[inst_id] is None:
            return False
        return self._timestamp - self._alert_timestamp[inst_id] > self._timeout

    @property
    def _threshold(self):
        return self._txn_delta_k * self._input_txn_rate.value

    def _is_degraded(self, inst_id):
        if len(self._instances) < 2:
            return False
        instance_ordered = self._ordered[inst_id]
        max_ordered = max(self._ordered[i] for i in self._instances)
        return (max_ordered - instance_ordered) > self._threshold

    def _statistics(self):
        return "txn rate {}, threshold {}, ordered: {}".\
               format(self._input_txn_rate.value,
                      self._threshold,
                      ", ".join([str(n) for n in self._ordered.values()]))
