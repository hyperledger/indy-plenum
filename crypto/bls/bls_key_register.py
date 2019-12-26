from abc import ABCMeta, abstractmethod


class BlsKeyRegister(metaclass=ABCMeta):
    @abstractmethod
    def get_key_by_name(self, node_name, pool_state_root_hash=None) -> object:
        '''
        Gets Public BLS key for a node with the specified name.
        The key is associated with a state of a pool as defined by pool_state_root_hash.
        If pool_state_root_hash is None, then the current committed root is used.
        :param node_name: node name
        :param pool_state_root_hash: pool state root hash get the key for, or None to use the current committed one
        :return: BLS public key
        '''
        pass

    @abstractmethod
    def get_pool_root_hash_committed(self):
        pass
