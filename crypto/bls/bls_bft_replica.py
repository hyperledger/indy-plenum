from abc import ABCMeta, abstractmethod

from crypto.bls.bls_bft import BlsBft
from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit


class BlsBftReplica(metaclass=ABCMeta):
    PPR_BLS_MULTISIG_WRONG = 1
    CM_BLS_SIG_WRONG = 2

    def __init__(self,
                 bls_bft: BlsBft,
                 is_master):
        self._bls_bft = bls_bft
        self._is_master = is_master

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
    def validate_commit(self, commit: Commit, sender, pre_prepare: PrePrepare):
        '''
        Validates Commit for correct BLS signatures.
        Raises SuspiciousNode exception if there are errors
        :param commit: commit to be validated
        :param sender: sender's Node name
        :param pre_prepare: PrePrepare associated with the Commit
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
    def process_order(self, key, quorums, pre_prepare: PrePrepare):
        '''
        Performs BLS-related logic when Ordering (for example, calculate a temporarily multi-sig by a current Node
          which will be replaced by Primary's multi-sig in  process_prepare).
        :param key: 3PC-key re;ated to the Ordered message
        :param quorums: quorums
        :param pre_prepare: PrePrepare associated with the ordered messages
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
    def update_commit(self, commit_params, pre_prepare: PrePrepare):
        '''
        Adds BLS-related parameters to be used for creation of a new Commit
        :param commit_params: a list of existing parameters
        :param pre_prepare: PrePrepare associated with the Commit
        :return: pre_prepare_params updated with BLS ones
        '''
        pass

    @abstractmethod
    def gc(self, key_3PC):
        """
        Do some cleaning if needed

        :param key_3PC: 3PC-key till which cleaning must be done
        """
        pass
