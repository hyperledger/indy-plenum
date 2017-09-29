from abc import ABCMeta, abstractmethod

from common.serializers.serialization import state_roots_serializer
from crypto.bls.bls_crypto import BlsCrypto
from crypto.bls.bls_key_register import BlsKeyRegister
from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit


class BlsBft(metaclass=ABCMeta):
    def __init__(self,
                 bls_crypto: BlsCrypto,
                 bls_key_register: BlsKeyRegister,
                 node_id,
                 is_master,
                 bls_store):
        self.bls_key_register = bls_key_register
        self.node_id = node_id

        self._bls_crypto = bls_crypto
        self._is_master = is_master
        self._bls_store = bls_store
        self._state_root_serializer = state_roots_serializer

    @abstractmethod
    def validate_pre_prepare(self, pre_prepare: PrePrepare, sender):
        '''
        Validates PrePrepare for correct BLS signatures.
        Raises SuspiciousNode exception if there are errors
        :param pre_prepare: pre-prepare to be validated
        :param sender: sender's Node name
        :return:
        '''
        pass

    @abstractmethod
    def validate_prepare(self, prepare: Prepare, sender):
        '''
        Validates Prepare for correct BLS signatures.
        Raises SuspiciousNode exception if there are errors
        :param prepare: prepare to be validated
        :param sender: sender's Node name
        :return:
        '''
        pass

    @abstractmethod
    def validate_commit(self, commit: Commit, sender, state_root_hash):
        '''
        Validates Commit for correct BLS signatures.
        Raises SuspiciousNode exception if there are errors
        :param commit: commit to be validated
        :param sender: sender's Node name
        :param state_root_hash: domain state root hash to validate BLS against
        :return:
        '''
        pass

    @abstractmethod
    def process_pre_prepare(self, pre_prepare: PrePrepare, sender):
        '''
        Performs BLS-related logic for a given PrePrepare (for example,
         saving multi-signature calculated by Pre-Prepare for last batches).
        :param pre_prepare: pre-prepare to be processed
        :param sender: sender's Node name
        :return:
        '''
        pass

    @abstractmethod
    def process_prepare(self, prepare: Prepare, sender):
        '''
        Performs BLS-related logic for a given Prepare
        :param prepare: prepare to be processed
        :param sender: sender's Node name
        :return:
        '''
        pass

    @abstractmethod
    def process_commit(self, commit: Commit, sender):
        '''
        Performs BLS-related logic for a given Commit (for example, saving BLS signatures from this Commit)
        :param commit: commit to be processed
        :param sender: sender's Node name
        :return:
        '''
        pass

    @abstractmethod
    def process_order(self, key, state_root_hash, quorums, ledger_id):
        '''
        Performs BLS-related logic when Ordering (for example, calculate a temporarily multi-sig by a current Node
          which will be replaced by Primary's multi-sig in  process_prepare).
        :param key: 3PC-key re;ated to the Ordered message
        :param state_root: domain state root hash to validate BLS against
        :param quorums: quorums
        :param ledger_id: ledger's ID
        :return:
        '''
        pass

    @abstractmethod
    def update_pre_prepare(self, pre_prepare_params, ledger_id):
        '''
        Adds BLS-related parameters to be used for creation of a new PrePrepare
        :param pre_prepare_params: a list of existing parameters
        :param ledger_id: ledger's ID
        :return: pre_prepare_params updated with BLS ones
        '''
        pass

    @abstractmethod
    def update_prepare(self, prepare_params, ledger_id):
        '''
        Adds BLS-related parameters to be used for creation of a new Prepare
        :param prepare_params: a list of existing parameters
        :param ledger_id: ledger's ID
        :return: pre_prepare_params updated with BLS ones
        '''
        pass

    @abstractmethod
    def update_commit(self, commit_params, state_root_hash, ledger_id):
        '''
        Adds BLS-related parameters to be used for creation of a new Commit
        :param commit_params: a list of existing parameters
        :param ledger_id: ledger's ID
        :return: pre_prepare_params updated with BLS ones
        '''
        pass

    @abstractmethod
    def gc(self, key_3PC):
        """
        Do some cleaning if needed

        :param key_3PC: 3PC-key
        """
        pass


class BlsValidationError(Exception):
    """
    BLS signature validation error
    """
