from crypto.bls.bls_bft import BlsBft
from crypto.bls.bls_crypto import BlsCrypto
from crypto.bls.bls_key_register import BlsKeyRegister
from plenum.common.exceptions import SuspiciousNode
from plenum.common.messages.node_messages import PrePrepare, Prepare
from plenum.common.types import f
from plenum.server.suspicion_codes import Suspicions


class BlsBftPlenum(BlsBft):
    def __init__(self, bls_crypto: BlsCrypto, bls_key_register: BlsKeyRegister):
        super().__init__(bls_crypto, bls_key_register)

    def validate_pre_prepare(self, pre_prepare: PrePrepare, sender):
        self._validate_multi_sig_pre_prepare(pre_prepare, sender)
        self._validate_sig_pre_prepare(pre_prepare, sender)

    def validate_prepare(self, prepare: Prepare, sender):
        self._validate_sig_prepare(prepare, sender)

    def sign_state(self, state_root) -> str:
        return self.bls_crypto.sign(state_root)

    def save_multi_sig(self, multi_sig_str):
        # TODO
        pass

    def _validate_multi_sig_pre_prepare(self, pre_prepare: PrePrepare, sender):
        if not f.BLS_MULTI_SIG.nm in pre_prepare:
            # TODO: It's optional for now
            return
        if not f.BLS_SIG_NODES.nm in pre_prepare:
            # TODO: It's optional for now
            return

        public_keys = []
        for node_id in pre_prepare.blsSigNodes:
            bls_key = self.bls_key_register.get_latest_key(node_id)
            # TODO: It's optional for now
            if bls_key:
                public_keys.append(bls_key)

        multi_sig = pre_prepare.blsMultiSig
        msg = pre_prepare.stateRootHash

        if not self.bls_crypto.verify_multi_sig(multi_sig, msg, public_keys):
            raise SuspiciousNode(
                sender, Suspicions.PPR_BLS_MULTISIG_WRONG, pre_prepare)

    def _validate_sig_pre_prepare(self, pre_prepare: PrePrepare, sender):
        if not f.BLS_SIG.nm in pre_prepare:
            # TODO: It's optional for now
            return

        pk = self.bls_key_register.get_latest_key(sender)
        if not pk:
            # TODO: It's optional for now
            return

        sig = pre_prepare.blsSig
        msg = pre_prepare.stateRootHash

        if not self.bls_crypto.verify_sig(sig, msg, pk):
            raise SuspiciousNode(
                sender, Suspicions.PPR_BLS_SIG_WRONG, pre_prepare)

    def _validate_sig_prepare(self, prepare: Prepare, sender):
        if not f.BLS_SIG.nm in prepare:
            # TODO: It's optional for now
            return

        pk = self.bls_key_register.get_latest_key(sender)
        if not pk:
            # TODO: It's optional for now
            return

        sig = prepare.blsSig
        msg = prepare.stateRootHash

        if not self.bls_crypto.verify_sig(sig, msg, pk):
            raise SuspiciousNode(
                sender, Suspicions.PR_BLS_SIG_WRONG, prepare)
