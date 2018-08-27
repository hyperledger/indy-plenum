
class QuotaControl:
    def __init__(self,
                 max_node_message_count_limit,
                 max_node_message_size_limit,
                 max_client_message_count_limit,
                 max_client_message_size_limit):
        self._max_node_message_count_limit = max_node_message_count_limit
        self._max_node_message_size_limit = max_node_message_size_limit
        self._max_client_message_count_limit = max_client_message_count_limit
        self._max_client_message_size_limit = max_client_message_size_limit
        self._node_quota_reached = False

    def received_node_messages(self, count: int, size: int):
        if count >= self._max_node_message_count_limit:
            self._node_quota_reached = True
            return

        if size >= self._max_node_message_size_limit:
            self._node_quota_reached = True
            return

        self._node_quota_reached = False

    def received_client_messages(self, count: int, size: int):
        pass

    @property
    def node_message_count_limit(self):
        return self._max_node_message_count_limit

    @property
    def node_message_size_limit(self):
        return self._max_node_message_size_limit

    @property
    def client_message_count_limit(self):
        if self._node_quota_reached:
            return 0
        return self._max_client_message_count_limit

    @property
    def client_message_size_limit(self):
        if self._node_quota_reached:
            return 0
        return self._max_client_message_size_limit
