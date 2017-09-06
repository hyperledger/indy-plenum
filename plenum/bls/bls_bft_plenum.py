from crypto.bls.bls_bft import BlsBft, BlsValidationError
from crypto.bls.bls_crypto import BlsCrypto
from crypto.bls.bls_key_register import BlsKeyRegister
from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit
from plenum.common.types import f
from plenum.common.util import compare_3PC_keys
from plenum.server.quorums import Quorums
from plenum.server.suspicion_codes import Suspicions
from stp_core.common.log import getlogger
from typing import Optional

logger = getlogger()


class BlsBftPlenum(BlsBft):
    def __init__(self,
                 bls_crypto: BlsCrypto,
                 bls_key_register: BlsKeyRegister,
                 node_id):
        super().__init__(bls_crypto, bls_key_register, node_id)
        # TODO: move it out of here
        self._signatures = {}

    def validate_pre_prepare(self, pre_prepare: PrePrepare, sender):
        if f.BLS_MULTI_SIG.nm not in pre_prepare:
            # TODO: It's optional for now
            # raise BlsValidationError("No signature found")
            return None
        multi_sig = pre_prepare.blsMultiSig[f.BLS_MULTI_SIG_VALUE.nm]
        participants = pre_prepare.blsMultiSig[f.BLS_MULTI_SIG_NODES.nm]
        state_root = pre_prepare.stateRootHash
        self.validate_multi_sig(multi_sig, participants, state_root)

    def validate_prepare(self, prepare: Prepare, sender):
        if f.BLS_SIG.nm not in prepare:
            # TODO: It's optional for now
            # raise BlsValidationError("No signature found")
            return None

        key_3PC = (prepare.viewNo, prepare.ppSeqNo)
        state_root = prepare.stateRootHash

        pk = self.bls_key_register.get_latest_key(self.get_node_name(sender))
        if not pk:
            raise BlsValidationError("No key for {} found".format(sender))
        sig = prepare.blsSig
        if not self.bls_crypto.verify_sig(sig, state_root, pk):
            raise BlsValidationError("Validation failed")
        if key_3PC not in self._signatures:
            self._signatures[key_3PC] = []
        self._signatures[key_3PC].append(sig)

    def validate_commit(self, commit: Commit, sender):
        # not required as of now
        pass

    def sign_state(self, state_root) -> str:
        return self.bls_crypto.sign(state_root)

    def calculate_multi_sig(self, key_3PC, quorums: Quorums) -> Optional[str]:
        if key_3PC not in self._signatures:
            return None
        bls_signatures = self._signatures[key_3PC]
        if not quorums.bls_signatures.is_reached(len(bls_signatures)):
            logger.debug(
                'Can not create bls signature for batch {}: '
                'There are only {} signatures, while {} required'
                .format(key_3PC,
                        len(bls_signatures),
                        quorums.bls_signatures.value))
            return None

        return self.bls_crypto.create_multi_sig(bls_signatures)

    def save_multi_sig_local(self, multi_sig: str, state_root, key_3PC):
        logger.info("SAVING MULTISIG!!! {}".format(multi_sig))

    def save_multi_sig_shared(self, pre_prepare: PrePrepare, key_3PC):

        if f.BLS_MULTI_SIG.nm not in pre_prepare:
            return
        multi_sig = pre_prepare.blsMultiSig[f.BLS_MULTI_SIG_VALUE.nm]
        state_root = pre_prepare.stateRootHash

        # TODO: store
        # TODO: support multiple multi-sigs for multiple previous batches

    def gc(self, key_3PC):
        keys_to_remove = []
        for key in self._signatures.keys():
            if compare_3PC_keys(key, key_3PC) >= 0:
                keys_to_remove.append(key)
        for key in keys_to_remove:
            self._signatures.pop(key, None)

    def validate_multi_sig(self, multi_sig: str, participants, state_root):
        public_keys = []
        for node_id in participants:
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
