from crypto.bls.bls_bft import BlsBft, BlsValidationError
from crypto.bls.bls_crypto import BlsCrypto
from crypto.bls.bls_key_register import BlsKeyRegister
from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit
from plenum.common.types import f
from plenum.common.util import compare_3PC_keys
from plenum.server.quorums import Quorums
from stp_core.common.log import getlogger
from typing import Optional
from plenum.bls.bls_store import BlsStore
from crypto.bls.bls_multi_signature import MultiSignature

logger = getlogger()


class BlsBftPlenum(BlsBft):
    def __init__(self,
                 bls_crypto: BlsCrypto,
                 bls_key_register: BlsKeyRegister,
                 node_id,
                 bls_store: BlsStore=None,
                 quorums=Quorums):
        super().__init__(bls_crypto, bls_key_register, node_id, quorums)
        self._signatures = {}
        self._bls_store = bls_store
        self._bls_store_add = self.__bls_save_empty \
            if bls_store is None else self.__bls_save

    def __bls_save(self, root_hash, multi_sig: MultiSignature):
        self._bls_store.put(root_hash, multi_sig)

    def __bls_save_empty(self, root_hash, multi_sig: MultiSignature):
        pass

    def _validate_signature(self, sender, bls_sig, state_root_hash):
        sender_node = self.get_node_name(sender)
        pk = self.bls_key_register.get_latest_key(sender_node)
        if not pk:
            raise BlsValidationError("No key for {} found".format(sender_node))
        if not self.bls_crypto.verify_sig(bls_sig, state_root_hash, pk):
            raise BlsValidationError("Validation failed")

    def validate_pre_prepare(self, pre_prepare: PrePrepare, sender):
        if f.BLS_MULTI_SIG.nm not in pre_prepare or \
           pre_prepare.blsMultiSig is None:
            return None
        multi_sig = pre_prepare.blsMultiSig
        self.validate_multi_sig(multi_sig, stable_state_root)

    def validate_commit(self, commit: Commit, sender):
        if f.BLS_SIG.nm not in commit:
            # TODO: It's optional for now
            # raise BlsValidationError("No signature found")
            return None
        self._validate_signature(sender, commit.blsSig, state_root)
        key_3PC = (commit.viewNo, commit.ppSeqNo)
        if key_3PC not in self._signatures:
            self._signatures[key_3PC] = {}
        self._signatures[key_3PC][self.get_node_name(sender)] = commit.blsSig

    def sign_state(self, state_root) -> str:
        return self.bls_crypto.sign(state_root)

    def calculate_multi_sig(self,
                            key_3PC) -> Optional[MultiSignature]:
        if key_3PC not in self._signatures:
            return None
        sigs_for_request = self._signatures[key_3PC]
        bls_signatures = list(sigs_for_request.values())
        participants = list(sigs_for_request.keys())

        if not self.quorums.bls_signatures.is_reached(len(bls_signatures)):
            logger.debug(
                'Can not create bls signature for batch {}: '
                'There are only {} signatures, while {} required'
                .format(key_3PC,
                        len(bls_signatures),
                        self.quorums.bls_signatures.value))
            return None

        sig = self.bls_crypto.create_multi_sig(bls_signatures)
        return MultiSignature(sig, participants, pool_state_root)

    def save_multi_sig_local(self,
                             multi_sig: MultiSignature,
                             state_root,
                             key_3PC):
        self._bls_store_add(state_root, multi_sig)

    def save_multi_sig_shared(self, pre_prepare: PrePrepare, key_3PC):

        if f.BLS_MULTI_SIG.nm not in pre_prepare:
            return
        if pre_prepare.blsMultiSig is None:
            return
        state_root, participants, multi_sig = pre_prepare.blsMultiSig
        if multi_sig is None:
            return

        self._bls_store_add(state_root, multi_sig)
        # TODO: support multiple multi-sigs for multiple previous batches

    def gc(self, key_3PC):
        keys_to_remove = []
        for key in self._signatures.keys():
            if compare_3PC_keys(key, key_3PC) >= 0:
                keys_to_remove.append(key)
        for key in keys_to_remove:
            self._signatures.pop(key, None)

    def validate_multi_sig(self, multi_sig: MultiSignature, state_root):
        public_keys = []
        for node_id in multi_sig.participants:
            bls_key = self.bls_key_register.get_latest_key(node_id)
            # TODO: It's optional for now
            if bls_key:
                public_keys.append(bls_key)
        if not self.bls_crypto.verify_multi_sig(multi_sig,
                                                state_root,
                                                public_keys):
            raise BlsValidationError("Multi-sig validation failed")

    @staticmethod
    def get_node_name(replica_name: str):
        # TODO: there is the same method in Replica
        # It should be moved to some util class
        return replica_name.split(":")[0]
