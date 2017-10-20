from typing import Optional

from common.serializers.serialization import state_roots_serializer
from crypto.bls.bls_bft import BlsBft
from crypto.bls.bls_bft_replica import BlsBftReplica
from crypto.bls.bls_multi_signature import MultiSignature
from plenum.bls.bls_bft_utils import create_full_root_hash
from plenum.common.constants import DOMAIN_LEDGER_ID, BLS_PREFIX
from plenum.common.exceptions import SuspiciousNode
from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit
from plenum.common.types import f
from plenum.common.util import compare_3PC_keys
from plenum.server.suspicion_codes import Suspicions
from stp_core.common.log import getlogger

logger = getlogger()


class BlsBftReplicaPlenum(BlsBftReplica):
    def __init__(self,
                 node_id,
                 bls_bft: BlsBft,
                 is_master):
        super().__init__(bls_bft, is_master)
        self.node_id = node_id
        self._signatures = {}
        self._bls_latest_multi_sig = None  # (pool_state_root, participants, sig)
        self._bls_latest_signed_root = None  # (pool_state_root, participants, sig)
        self.state_root_serializer = state_roots_serializer

    def _can_process_ledger(self, ledger_id):
        return ledger_id == DOMAIN_LEDGER_ID

    # ----VALIDATE----

    def validate_pre_prepare(self, pre_prepare: PrePrepare, sender):
        if f.BLS_MULTI_SIG.nm not in pre_prepare or \
                pre_prepare.blsMultiSig is None:
            return

        # if we have multi-sig, then we must have the corresponded state root as well
        if f.BLS_MULTI_SIG_STATE_ROOT.nm not in pre_prepare or \
                pre_prepare.blsMultiSigStateRoot is None:
            raise SuspiciousNode(sender,
                                 Suspicions.PPR_NO_BLS_MULTISIG_STATE,
                                 pre_prepare)

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
            return

        if not self._validate_signature(sender, commit.blsSig, state_root_hash):
            raise SuspiciousNode(sender,
                                 Suspicions.CM_BLS_SIG_WRONG,
                                 commit)

    # ----CREATE/UPDATE----

    def update_pre_prepare(self, pre_prepare_params, ledger_id):
        if not self._can_process_ledger(ledger_id):
            return pre_prepare_params

        if not self._bls_latest_multi_sig:
            return pre_prepare_params
        if not self._bls_latest_signed_root:
            return pre_prepare_params

        pre_prepare_params.append(
            [self._bls_latest_multi_sig.signature, self._bls_latest_multi_sig.participants,
             self._bls_latest_multi_sig.pool_state_root]
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

        if not self._bls_bft.can_sign_bls():
            logger.debug("{}{} can not sign COMMIT {} for state {}: No BLS keys"
                         .format(BLS_PREFIX, self, commit_params, state_root_hash))
            return commit_params

        bls_signature = self._sign_state(state_root_hash)
        logger.debug("{}{} signed COMMIT {} for state {} with sig {}"
                     .format(BLS_PREFIX, self, commit_params, state_root_hash, bls_signature))
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

        if not self._can_calculate_multi_sig(key, quorums):
            return

        # calculate signature always to keep master and non-master in sync
        # but save on master only
        bls_multi_sig = self._calculate_multi_sig(key)

        if not self._is_master:
            return

        self._save_multi_sig_local(bls_multi_sig, state_root)

        self._bls_latest_multi_sig = bls_multi_sig
        self._bls_latest_signed_root = state_root

    # ----GC----

    def gc(self, key_3PC):
        keys_to_remove = []
        for key in self._signatures.keys():
            if compare_3PC_keys(key, key_3PC) >= 0:
                keys_to_remove.append(key)
        for key in keys_to_remove:
            self._signatures.pop(key, None)

    # ----MULT_SIG----

    def _create_multi_sig_value(self, state_root_hash, pool_state_root_hash):
        return create_full_root_hash(state_root_hash, pool_state_root_hash)

    def _validate_signature(self, sender, bls_sig, state_root_hash):
        sender_node = self.get_node_name(sender)
        pk = self._bls_bft.bls_key_register.get_key_by_name(sender_node)
        if not pk:
            return False
        pool_state_root_hash_str = self.state_root_serializer.serialize(
            bytes(self._bls_bft.bls_key_register.get_pool_root_hash_committed()))
        message = self._create_multi_sig_value(state_root_hash,
                                               pool_state_root_hash_str)
        return self._bls_bft.bls_crypto_verifier.verify_sig(bls_sig, message, pk)

    def _validate_multi_sig(self, multi_sig: MultiSignature, state_root):
        public_keys = []
        for node_name in multi_sig.participants:
            pool_root_hash = self.state_root_serializer.deserialize(multi_sig.pool_state_root)
            bls_key = self._bls_bft.bls_key_register.get_key_by_name(node_name,
                                                                     pool_root_hash)
            # TODO: It's optional for now
            if bls_key:
                public_keys.append(bls_key)
        message = self._create_multi_sig_value(state_root,
                                               multi_sig.pool_state_root)
        return self._bls_bft.bls_crypto_verifier.verify_multi_sig(multi_sig.signature,
                                                                  message,
                                                                  public_keys)

    def _sign_state(self, state_root_hash):
        pool_root_hash_ser = self.state_root_serializer.serialize(
            bytes(self._bls_bft.bls_key_register.get_pool_root_hash_committed()))
        message = self._create_multi_sig_value(state_root_hash,
                                               pool_root_hash_ser)
        return self._bls_bft.bls_crypto_signer.sign(message)

    def _can_calculate_multi_sig(self,
                                 key_3PC,
                                 quorums) -> bool:
        if key_3PC not in self._signatures:
            return False

        sigs_for_request = self._signatures[key_3PC]
        bls_signatures = list(sigs_for_request.values())
        if not quorums.bls_signatures.is_reached(len(bls_signatures)):
            logger.debug(
                '{}Can not create bls signature for batch {}: '
                'There are only {} signatures, while {} required'.format(BLS_PREFIX,
                                                                         key_3PC,
                                                                         len(bls_signatures),
                                                                         quorums.bls_signatures.value))
            return False

        return True

    def _calculate_multi_sig(self, key_3PC) -> Optional[MultiSignature]:
        sigs_for_request = self._signatures[key_3PC]
        bls_signatures = list(sigs_for_request.values())
        participants = list(sigs_for_request.keys())

        sig = self._bls_bft.bls_crypto_verifier.create_multi_sig(bls_signatures)
        pool_root_hash_ser = self.state_root_serializer.serialize(
            bytes(self._bls_bft.bls_key_register.get_pool_root_hash_committed()))
        return MultiSignature(sig,
                              participants,
                              pool_root_hash_ser)

    def _save_multi_sig_local(self,
                              multi_sig: MultiSignature,
                              state_root):
        self._bls_store_add(state_root, multi_sig)
        logger.debug("{}{} saved multi signature {} for root {} (locally calculated)"
                     .format(BLS_PREFIX, self, multi_sig, state_root))

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
        logger.debug("{}{} saved multi signature {} for root {} (calculated by Primary)"
                     .format(BLS_PREFIX, self, multi_sig, state_root))
        # TODO: support multiple multi-sigs for multiple previous batches

    def _bls_store_add(self, root_hash, multi_sig: MultiSignature):
        if self._bls_bft.bls_store:
            self._bls_bft.bls_store.put(root_hash, multi_sig)

    @staticmethod
    def get_node_name(replica_name: str):
        # TODO: there is the same method in Replica
        # It should be moved to some util class
        return replica_name.split(":")[0]

    def __str__(self, *args, **kwargs):
        return self.node_id
