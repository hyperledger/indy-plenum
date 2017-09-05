from crypto.bls.bls_bft import BlsBft, BlsValidationError
from crypto.bls.bls_crypto import BlsCrypto
from crypto.bls.bls_key_register import BlsKeyRegister
from plenum.common.exceptions import SuspiciousNode
from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit
from plenum.common.types import f
from plenum.common.util import compare_3PC_keys
from plenum.server.quorums import Quorums
from plenum.server.suspicion_codes import Suspicions
from stp_core.common.log import getlogger
from typing import Optional

logger = getlogger()


class BlsBftPlenum(BlsBft):
    def __init__(self, bls_crypto: BlsCrypto, bls_key_register: BlsKeyRegister, node_id):
        super().__init__(bls_crypto, bls_key_register, node_id)
        self._commits = {}

    def validate_pre_prepare(self, pre_prepare: PrePrepare, sender):
        self._validate_multi_sig_pre_prepare(pre_prepare, sender)

    def validate_prepare(self, prepare: Prepare, sender):
        # not needed now
        pass

    def validate_commit(self, key_3PC, commit: Commit, sender, state_root):
        if self._validate_sig_commit(commit, sender, state_root):
            if key_3PC not in self._commits:
                self._commits[key_3PC] = []
            self._commits[key_3PC].append(commit)

    def sign_state(self, state_root) -> str:
        return self.bls_crypto.sign(state_root)

    def calculate_multi_sig(self, key_3PC, quorums: Quorums) -> Optional[str]:
        if key_3PC not in self._commits:
            return None

        bls_signatures = []
        for commit in self._commits[key_3PC]:
            if f.BLS_SIG.nm in commit:
                bls_signatures.append(commit.blsSig)

        if not quorums.bls_signatures.is_reached(len(bls_signatures)):
            logger.debug(
                'Can not create bls signature for batch {}: '
                'COMMITs have only {} signatures, while {} required'
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
        for key in self._commits.keys():
            if compare_3PC_keys(key, key_3PC) >= 0:
                keys_to_remove.append(key)

        for key in keys_to_remove:
            self._commits.pop(key, None)

    def _validate_multi_sig_pre_prepare(self, pre_prepare: PrePrepare, sender):
        if f.BLS_MULTI_SIG.nm not in pre_prepare:
            # TODO: It's optional for now
            return
        multi_sig = pre_prepare.blsMultiSig

        public_keys = []
        for node_id in multi_sig[f.BLS_MULTI_SIG_NODES.nm]:
            bls_key = self.bls_key_register.get_latest_key(node_id)
            # TODO: It's optional for now
            if bls_key:
                public_keys.append(bls_key)

        multi_sig = multi_sig[f.BLS_MULTI_SIG_VALUE.nm]
        msg = pre_prepare.stateRootHash

        if not self.bls_crypto.verify_multi_sig(multi_sig, msg, public_keys):
            raise SuspiciousNode(
                sender, Suspicions.PPR_BLS_MULTISIG_WRONG, pre_prepare)

    def _validate_sig_commit(self, commit: Commit, sender, state_root):
        if f.BLS_SIG.nm not in commit:
            raise BlsValidationError("No signature found")
        pk = self.bls_key_register.get_latest_key(self.get_node_name(sender))
        if not pk:
            raise BlsValidationError("No key for {} found".format(sender))
        sig = commit.blsSig
        if not self.bls_crypto.verify_sig(sig, state_root, pk):
            raise SuspiciousNode(sender, Suspicions.CM_BLS_SIG_WRONG, commit)

    @staticmethod
    def get_node_name(replicaName: str):
        # TODO: there is the same method in Replica
        # It should be moved to some util class
        return replicaName.split(":")[0]
