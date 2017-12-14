from plenum.common.constants import PRE_REPLY_SENT, REPLY
from plenum.common.messages.node_messages import Reply, ObservedData
from plenum.server.node import Node
from plenum.server.observer.observer_sync_policy import ObserverSyncPolicy, ObserverSyncPolicyType


class ObserverSyncPolicyEachReply(ObserverSyncPolicy):
    def __init__(self, observable, node: Node) -> None:
        super().__init__(observable)
        node.register_hook(PRE_REPLY_SENT,
                           self.send_reply_to_observers())

    def can_process(self, observer_policy_type: ObserverSyncPolicyType):
        return observer_policy_type == ObserverSyncPolicyType.EACH_REPLY

    def send_reply_to_observers(self):
        def do_send_reply_to_observers(reply: Reply):
            msg = ObservedData(REPLY, reply)
            self._observable.send_to_observers(msg, self._observers)

        return do_send_reply_to_observers
