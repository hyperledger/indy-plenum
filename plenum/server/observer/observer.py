from abc import abstractmethod, ABCMeta
from logging import getLogger

logger = getLogger()


class Observer(metaclass=ABCMeta):
    def __init__(self, sync_policies) -> None:
        self.__sync_policies = {policy.policy_type: policy for policy in sync_policies}

    def apply_data(self, msg):
        if msg.msg_type not in self.__sync_policies:
            logger.warning('Unsupported Observer Data type {}'.format(msg.msg_type))
            return
        self.__sync_policies[msg.msg_type].apply_data(msg)

    @property
    @abstractmethod
    def observer_remote_id(self) -> str:
        pass
