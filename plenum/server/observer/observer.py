from abc import abstractmethod, ABCMeta
from logging import getLogger

logger = getLogger()


class Observer(metaclass=ABCMeta):
    '''
    The abstract Observer interface.
    A number of observer strategies (policies) can be attached which deal with the
    incoming ObservedData messages (in fact, the policies should match ObservedData msg type and ObservableSyncPolicies)
    '''

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
