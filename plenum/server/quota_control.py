from abc import ABC, abstractmethod

from stp_zmq.zstack import Quota


class QuotaControl(ABC):
    @abstractmethod
    def update_state(self, state: dict):
        pass

    @property
    @abstractmethod
    def node_quota(self) -> Quota:
        pass

    @property
    @abstractmethod
    def client_quota(self) -> Quota:
        pass


class StaticQuotaControl(QuotaControl):
    def __init__(self, node_quota: Quota, client_quota: Quota):
        self._node_quota = node_quota
        self._client_quota = client_quota

    def update_state(self, state: dict):
        pass

    @property
    def node_quota(self) -> Quota:
        return self._node_quota

    @property
    def client_quota(self) -> Quota:
        return self._client_quota


class CompositeQuotaControl(QuotaControl):
    def __init__(self, *args):
        self._controls = [*args]

    def update_state(self, state: dict):
        for qc in self._controls:
            qc.update_state(state)

    @property
    def node_quota(self) -> Quota:
        return Quota(count=min(qc.node_quota.count for qc in self._controls),
                     size=min(qc.node_quota.size for qc in self._controls))

    @property
    def client_quota(self) -> Quota:
        return Quota(count=min(qc.client_quota.count for qc in self._controls),
                     size=min(qc.client_quota.size for qc in self._controls))


class RequestQueueQuotaControl(QuotaControl):
    def __init__(self,
                 max_request_queue_size: int,
                 max_node_quota: Quota,
                 max_client_quota: Quota):
        self._max_request_queue_size = max_request_queue_size
        self._max_node_quota = max_node_quota
        self._max_client_quota = max_client_quota
        self._request_queue_overflow = False

    def update_state(self, state: dict):
        self._request_queue_overflow = state.get('request_queue_size', 0) >= self._max_request_queue_size

    @property
    def node_quota(self) -> Quota:
        return self._max_node_quota

    @property
    def client_quota(self) -> Quota:
        return Quota(count=0, size=0) if self._request_queue_overflow else self._max_client_quota
