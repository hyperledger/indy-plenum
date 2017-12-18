from plenum.common.constants import REPLY
from plenum.common.messages.node_messages import ObservedData
from plenum.server.observer.observer_sync_policy import ObserverSyncPolicy


class ObserverSyncPolicyEachReply(ObserverSyncPolicy):
    def __init__(self, node) -> None:
        super().__init__()
        self._node = node

    def apply_data(self, msg: ObservedData):
        assert msg.msg_type == REPLY, \
            "'Each Reply' policy is used for Observer Data message of type {}".format(msg.msg_type)
        req = self.requests[reqKey].finalised
        self.applyReq(req, msg.ppTime)
        state_root = self.stateRootHash(msg.ledgerId, isCommitted=False)
        self.onBatchCreated(msg.ledgerId, state_root)
