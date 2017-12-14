from abc import abstractmethod, ABCMeta

from plenum.common.messages.node_messages import ObservedData
from plenum.server.node import Node
from plenum.server.observer.observer_sync_policy import ObserverSyncPolicyType
from plenum.server.observer.observer_sync_policy_each_req import ObserverSyncPolicyEachReply


class Observable(metaclass=ABCMeta):
    def __init__(self) -> None:
        self.__sync_policies = {}

    def register_observable(self, node: Node):
        # TODO: support other policies
        self.__sync_policies = {
            ObserverSyncPolicyType.EACH_REPLY: ObserverSyncPolicyEachReply(self, node)
        }

    def add_observer(self, observer_remote_id: str,
                     observer_policy_type: ObserverSyncPolicyType):
        assert len(self.__sync_policies) > 0, "call `register_observable` first"
        for sync_policy in self.__sync_policies.values():
            sync_policy.add_observer(observer_remote_id, observer_policy_type)

    def remove_observer(self, observer_remote_id):
        assert len(self.__sync_policies) > 0, "call `register_observable` first"
        for sync_policy in self.__sync_policies.values():
            sync_policy.remove_observer(observer_remote_id)

    def get_observers(self, observer_policy_type: ObserverSyncPolicyType):
        policy = self._get_policy(observer_policy_type)
        if not policy:
            return []
        return policy.get_observers()

    def _get_policy(self, observer_policy_type: ObserverSyncPolicyType):
        if observer_policy_type not in self.__sync_policies:
            return None
        return self.__sync_policies[observer_policy_type]

    @abstractmethod
    def send_to_observers(self, msg: ObservedData, observer_remote_ids):
        # TODO: support other ways of connection to observers
        pass
