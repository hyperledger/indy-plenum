from abc import ABCMeta, abstractmethod


class BlsKeyRegister(metaclass=ABCMeta):
    @abstractmethod
    def load_latest_keys(self, *args):
        pass

    @abstractmethod
    def add_latest_key(self, node_id, bls_key_str):
        pass

    @abstractmethod
    def remove_latest_key(self, node_id):
        pass

    @abstractmethod
    def get_latest_key(self, node_id):
        pass

    @abstractmethod
    def get_key_for_multisig(self, node_id, state_root):
        pass
