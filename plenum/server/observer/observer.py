from abc import abstractmethod, ABCMeta
from logging import getLogger

from plenum.common.constants import REPLY
from plenum.common.messages.node_messages import ObservedData
from plenum.server.node import Node
from plenum.server.observer.observer_sync_policy_each_reply import ObserverSyncPolicyEachReply

logger = getLogger()


class Observer(metaclass=ABCMeta):
    def __init__(self) -> None:
        self.__sync_policies = {}

    def register_observer(self, node: Node):
        # TODO: support other policies
        self.__sync_policies[REPLY] = ObserverSyncPolicyEachReply(node)

    def apply_data(self, msg: ObservedData):
        if msg.msg_type not in self.__sync_policies:
            logger.warning('Unsupported Observer Data type {}'.format(msg.msg_type))
        self.__sync_policies[msg.msg_type].apply_data(msg)

    @abstractmethod
    @property
    def observer_remote_id(self) -> str:
        pass
