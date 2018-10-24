from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Sequence

from plenum.common.moving_average import EMAEventFrequencyEstimator


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
        self._ordered = defaultdict(lambda: 0)
        self._timestamp = start_time
        self._alert_timestamp = {inst: None for inst in self._instances}
        self._input_txn_rate = EMAEventFrequencyEstimator(start_time, input_rate_reaction_half_time)

    def add_instance(self, inst_id):
        self._instances.add(inst_id)

    def remove_instance(self, inst_id):
        self._instances.remove(inst_id)

    def reset(self):
        self._alert_timestamp = {inst: None for inst in self._instances}
        self._ordered.clear()

    def update_time(self, timestamp: float):
        self._timestamp = timestamp
        self._input_txn_rate.update_time(timestamp)
        for inst_id in self._instances:
            if not self._is_degraded(inst_id):
                self._alert_timestamp[inst_id] = None
            elif not self._alert_timestamp.get(inst_id, None):
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

    def _is_degraded(self, inst_id):
        if len(self._instances) < 2:
            return False
        instance_ordered = self._ordered[inst_id]
        max_ordered = max(self._ordered[i] for i in self._instances)
        return (max_ordered - instance_ordered) > self._txn_delta_k * self._input_txn_rate.value
