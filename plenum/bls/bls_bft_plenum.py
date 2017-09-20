from typing import Optional

from crypto.bls.bls_bft import BlsBft
from crypto.bls.bls_crypto import BlsCrypto
from crypto.bls.bls_key_register import BlsKeyRegister
from crypto.bls.bls_multi_signature import MultiSignature
from plenum.bls.bls_store import BlsStore
from plenum.common.exceptions import SuspiciousNode
from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit
from plenum.common.types import f
from plenum.common.util import compare_3PC_keys
from plenum.server.suspicion_codes import Suspicions
from state.state import State
from stp_core.common.log import getlogger

logger = getlogger()


class BlsBftPlenum(BlsBft):
    def __init__(self,
                 bls_crypto: BlsCrypto,
                 bls_key_register: BlsKeyRegister,
                 node_id,
                 is_master,
                 pool_state: State,
                 bls_store: BlsStore = None):
        super().__init__(bls_crypto, bls_key_register, node_id, is_master, pool_state, bls_store)
        self._signatures = {}
        self._bls_latest_multi_sig = None  # (pool_state_root, participants, sig)
        self._bls_latest_signed_root = None  # (pool_state_root, participants, sig)

    def _can_process_ledger(self, ledger_id):
        # return ledger_id == DOMAIN_LEDGER_ID
        return True

    # ----VALIDATE----

    def validate_pre_prepare(self, pre_prepare: PrePrepare, sender):
        if (f.BLS_MULTI_SIG.nm not in pre_prepare or
                    pre_prepare.blsMultiSig is None or
                    f.BLS_MULTI_SIG_STATE_ROOT.nm not in pre_prepare or
                    pre_prepare.blsMultiSigStateRoot is None):
            return None
        multi_sig = MultiSignature(*pre_prepare.blsMultiSig)
        state_root = pre_prepare.blsMultiSigStateRoot
        if not self._validate_multi_sig(multi_sig, state_root):
            raise SuspiciousNode(sender,
                                 Suspicions.PPR_BLS_MULTISIG_WRONG,
                                 pre_prepare)

    def validate_prepare(self, prepare: Prepare, sender):
        pass

    def validate_commit(self, commit: Commit, sender, state_root_hash):
        if f.BLS_SIG.nm not in commit:
            # TODO: It's optional for now
            # raise BlsValidationError("No signature found")
            return None

        if not self._validate_signature(sender, commit.blsSig, state_root_hash):
            raise SuspiciousNode(sender,
                                 Suspicions.CM_BLS_SIG_WRONG,
                                 commit)

    def _validate_signature(self, sender, bls_sig, state_root_hash):
        sender_node = self.get_node_name(sender)
        pk = self.bls_key_register.get_latest_key(sender_node)
        if not pk:
            return False
        return self._bls_crypto.verify_sig(bls_sig, state_root_hash, pk)

    def _validate_multi_sig(self, multi_sig: MultiSignature, state_root):
        public_keys = []
        for node_id in multi_sig.participants:
            bls_key = self.bls_key_register.get_latest_key(node_id)
            # TODO: It's optional for now
            if bls_key:
                public_keys.append(bls_key)
        return self._bls_crypto.verify_multi_sig(multi_sig.signature,
                                                 state_root,
                                                 public_keys)

    # ----CREATE/UPDATE----

    def update_pre_prepare(self, pre_prepare_params, ledger_id):
        if not self._can_process_ledger(ledger_id):
            return pre_prepare_params

        if not self._bls_latest_multi_sig:
            return pre_prepare_params
        if not self._bls_latest_signed_root:
            return pre_prepare_params

        pre_prepare_params.append(
            (self._bls_latest_multi_sig.signature, self._bls_latest_multi_sig.participants,
             self._bls_latest_multi_sig.pool_state_root)
        )
        pre_prepare_params.append(self._bls_latest_signed_root)
        self._bls_latest_multi_sig = None
        self._bls_latest_signed_root = None
        # Send signature in COMMITs only

        return pre_prepare_params

    def update_prepare(self, prepare_params, ledger_id):
        # Send BLS signature in COMMITs only
        return prepare_params

    def update_commit(self, commit_params, state_root_hash, ledger_id):
        if not self._can_process_ledger(ledger_id):
            return commit_params

        bls_signature = self._bls_crypto.sign(state_root_hash)
        commit_params.append(bls_signature)
        return commit_params

    # ----PROCESS----

    def process_pre_prepare(self, pre_prepare: PrePrepare, sender):
        # does not matter which ledger id is current PPR for
        # mult-sig is for domain ledger anyway
        self._save_multi_sig_shared(pre_prepare)

    def process_prepare(self, prepare: Prepare, sender):
        pass

    def process_commit(self, commit: Commit, sender):
        if f.BLS_SIG.nm not in commit:
            return
        if commit.blsSig is None:
            return

        key_3PC = (commit.viewNo, commit.ppSeqNo)
        if key_3PC not in self._signatures:
            self._signatures[key_3PC] = {}
        self._signatures[key_3PC][self.get_node_name(sender)] = commit.blsSig

    def process_order(self, key, state_root, quorums, ledger_id):
        if not self._can_process_ledger(ledger_id):
            return

        # calculate signature always to keep master and non-master in sync
        # but save on master only
        bls_multi_sig = self._calculate_multi_sig(
            key, quorums, self._get_pool_root_hash_committed())
        if bls_multi_sig is None:
            return

        if not self._is_master:
            return

        self._save_multi_sig_local(bls_multi_sig, state_root)
        logger.debug("{} saved multi signature for root"
                     .format(self, state_root))

        self._bls_latest_multi_sig = bls_multi_sig
        self._bls_latest_signed_root = state_root

    def _calculate_multi_sig(self,
                             key_3PC,
                             quorums,
                             pool_state_root) -> Optional[MultiSignature]:
        if key_3PC not in self._signatures:
            return None
        sigs_for_request = self._signatures[key_3PC]
        bls_signatures = list(sigs_for_request.values())
        participants = list(sigs_for_request.keys())

        if not quorums.bls_signatures.is_reached(len(bls_signatures)):
            logger.debug(
                'Can not create bls signature for batch {}: '
                'There are only {} signatures, while {} required'.format(key_3PC,
                                                                         len(bls_signatures),
                                                                         quorums.bls_signatures.value))
            return None

        sig = self._bls_crypto.create_multi_sig(bls_signatures)
        return MultiSignature(sig, participants, pool_state_root)

    def _save_multi_sig_local(self,
                              multi_sig: MultiSignature,
                              state_root):
        self._bls_store_add(state_root, multi_sig)

    def _save_multi_sig_shared(self, pre_prepare: PrePrepare):

        if f.BLS_MULTI_SIG.nm not in pre_prepare:
            return
        if pre_prepare.blsMultiSig is None:
            return
        if f.BLS_MULTI_SIG_STATE_ROOT.nm not in pre_prepare:
            return
        if pre_prepare.blsMultiSigStateRoot is None:
            return

        state_root = pre_prepare.blsMultiSigStateRoot
        multi_sig = MultiSignature(*pre_prepare.blsMultiSig)
        self._bls_store_add(state_root, multi_sig)
        # TODO: support multiple multi-sigs for multiple previous batches

        logger.debug("{} saved shared multi signature for root"
                     .format(self, state_root))

    def _bls_store_add(self, root_hash, multi_sig: MultiSignature):
        if self._bls_store:
            self._bls_store.put(root_hash, multi_sig)

    # ----GC----

    def gc(self, key_3PC):
        keys_to_remove = []
        for key in self._signatures.keys():
            if compare_3PC_keys(key, key_3PC) >= 0:
                keys_to_remove.append(key)
        for key in keys_to_remove:
            self._signatures.pop(key, None)

    @staticmethod
    def get_node_name(replica_name: str):
        # TODO: there is the same method in Replica
        # It should be moved to some util class
        return replica_name.split(":")[0]
