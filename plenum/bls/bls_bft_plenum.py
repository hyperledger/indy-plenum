from typing import Sequence

from crypto.bls.bls_bft import BlsBft
from crypto.bls.bls_crypto import BlsCrypto
from crypto.bls.bls_key_register import BlsKeyRegister
from plenum.common.exceptions import SuspiciousNode
from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit
from plenum.common.types import f
from plenum.server.quorums import Quorums
from plenum.server.suspicion_codes import Suspicions
from stp_core.common.log import getlogger

logger = getlogger()


class BlsBftPlenum(BlsBft):
    def __init__(self, bls_crypto: BlsCrypto, bls_key_register: BlsKeyRegister, node_id):
        super().__init__(bls_crypto, bls_key_register, node_id)

    def validate_pre_prepare(self, pre_prepare: PrePrepare, sender):
        self._validate_multi_sig_pre_prepare(pre_prepare, sender)

    def validate_prepare(self, prepare: Prepare, sender):
        # not needed now
        pass

    def validate_commit(self, commit: Commit, sender, state_root):
        self._validate_sig_commit(commit, sender, state_root)

    def sign_state(self, state_root) -> str:
        return self.bls_crypto.sign(state_root)

    def calculate_multi_sig(self, key_3PC, quorums: Quorums, commits: Sequence[Commit]) -> str:
        bls_signatures = []
        for commit in commits:
            if f.BLS_SIG.nm in commit:
                bls_signatures.append(commit.blsSig)

        if not quorums.bls_signatures.is_reached(len(bls_signatures)):
            logger.debug(
                'Can not create bls signature for batch {}: COMMITs have only {} signatures, while {} required'.
                    format(key_3PC,
                           len(
                               bls_signatures),
                           quorums.bls_signatures.value))
            return None

        return self.bls_crypto.create_multi_sig(bls_signatures)

    def save_multi_sig_local(self, multi_sig: str):
        pass

    def save_multi_sig_shared(self, multi_sig: str):
        pass

    def _validate_multi_sig_pre_prepare(self, pre_prepare: PrePrepare, sender):
        if not f.BLS_MULTI_SIG.nm in pre_prepare:
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
        if not f.BLS_SIG.nm in commit:
            # TODO: It's optional for now
            return

        pk = self.bls_key_register.get_latest_key(sender)
        if not pk:
            # TODO: It's optional for now
            return

        sig = commit.blsSig
        if not self.bls_crypto.verify_sig(sig, state_root, pk):
            raise SuspiciousNode(
                sender, Suspicions.CM_BLS_SIG_WRONG, commit)
