from stp_zmq.zstack import Quota


class QuotaControl:
    def __init__(self,
                 dynamic: bool,
                 max_request_queue_size: int,
                 max_node_quota: Quota,
                 max_client_quota: Quota):
        self._dynamic = dynamic
        self._max_request_queue_size = max_request_queue_size
        self._max_node_quota = max_node_quota
        self._max_client_quota = max_client_quota
        self._request_queue_overflow = False

    def set_request_queue_len(self, value):
        self._request_queue_overflow = value >= self._max_request_queue_size

    @property
    def node_quota(self) -> Quota:
        return self._max_node_quota

    @property
    def client_quota(self) -> Quota:
        if not self._dynamic:
            return self._max_client_quota
        return Quota(count=0, size=0) if self._request_queue_overflow else self._max_client_quota
