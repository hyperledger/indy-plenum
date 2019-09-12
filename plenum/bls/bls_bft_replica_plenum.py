from typing import Optional

from common.serializers.serialization import state_roots_serializer
from crypto.bls.bls_bft import BlsBft
from crypto.bls.bls_bft_replica import BlsBftReplica
from crypto.bls.bls_multi_signature import MultiSignature, MultiSignatureValue
from plenum.common.constants import DOMAIN_LEDGER_ID, BLS_PREFIX, POOL_LEDGER_ID, AUDIT_LEDGER_ID, TXN_PAYLOAD, \
    TXN_PAYLOAD_DATA, AUDIT_TXN_LEDGER_ROOT, AUDIT_TXN_STATE_ROOT, AUDIT_TXN_PP_SEQ_NO
from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit
from plenum.common.metrics_collector import MetricsCollector, NullMetricsCollector, measure_time, MetricsName
from plenum.common.types import f
from plenum.common.util import compare_3PC_keys
from plenum.server.database_manager import DatabaseManager
from stp_core.common.log import getlogger

logger = getlogger()


class BlsBftReplicaPlenum(BlsBftReplica):
    def __init__(self,
                 node_id,
                 bls_bft: BlsBft,
                 is_master,
                 database_manager: DatabaseManager,
                 metrics: MetricsCollector = NullMetricsCollector()):
        super().__init__(bls_bft, is_master)
        self._all_bls_latest_multi_sigs = None
        self.node_id = node_id
        self._database_manager = database_manager
        self._signatures = {}
        self._all_signatures = {}
        self._bls_latest_multi_sig = None  # MultiSignature
        self.state_root_serializer = state_roots_serializer
        self.metrics = metrics

    def _can_process_ledger(self, ledger_id):
        # enable BLS for all ledgers
        return True

    # ----VALIDATE----

    @measure_time(MetricsName.BLS_VALIDATE_PREPREPARE_TIME)
    def validate_pre_prepare(self, pre_prepare: PrePrepare, sender):
        if f.BLS_MULTI_SIGS.nm in pre_prepare and pre_prepare.blsMultiSigs:
            multi_sigs = pre_prepare.blsMultiSigs
            for sig in multi_sigs:
                multi_sig = MultiSignature.from_list(*sig)
                if not self._validate_multi_sig(multi_sig):
                    return BlsBftReplica.PPR_BLS_MULTISIG_WRONG

        if f.BLS_MULTI_SIG.nm not in pre_prepare or \
                pre_prepare.blsMultiSig is None:
            return

        multi_sig = MultiSignature.from_list(*pre_prepare.blsMultiSig)
        if not self._validate_multi_sig(multi_sig):
            return BlsBftReplica.PPR_BLS_MULTISIG_WRONG

    def validate_prepare(self, prepare: Prepare, sender):
        pass

    @measure_time(MetricsName.BLS_VALIDATE_COMMIT_TIME)
    def validate_commit(self, commit: Commit, sender, pre_prepare: PrePrepare):
        if f.BLS_SIGS.nm in commit:
            audit_txn = self._get_correct_audit_transaction(pre_prepare)
            if audit_txn:
                audit_payload = audit_txn[TXN_PAYLOAD][TXN_PAYLOAD_DATA]
                for lid, sig in commit.blsSigs.items():
                    lid = int(lid)
                    if lid not in audit_payload[AUDIT_TXN_STATE_ROOT] or lid not in audit_payload[AUDIT_TXN_LEDGER_ROOT]:
                        return BlsBftReplicaPlenum.CM_BLS_SIG_WRONG
                    if not self._validate_signature(sender, sig,
                                                    BlsBftReplicaPlenum._create_fake_pre_prepare_for_multi_sig(
                                                        lid,
                                                        audit_payload[AUDIT_TXN_STATE_ROOT][lid],
                                                        audit_payload[AUDIT_TXN_LEDGER_ROOT][lid],
                                                        pre_prepare
                                                    )):
                        return BlsBftReplicaPlenum.CM_BLS_SIG_WRONG
        if f.BLS_SIG.nm not in commit:
            # TODO: It's optional for now
            return

        if not self._validate_signature(sender, commit.blsSig, pre_prepare):
            return BlsBftReplica.CM_BLS_SIG_WRONG

    # ----CREATE/UPDATE----

    @measure_time(MetricsName.BLS_UPDATE_PREPREPARE_TIME)
    def update_pre_prepare(self, pre_prepare_params, ledger_id):
        if not self._can_process_ledger(ledger_id):
            return pre_prepare_params

        if self._bls_latest_multi_sig is not None:
            pre_prepare_params.append(self._bls_latest_multi_sig.as_list())
            self._bls_latest_multi_sig = None

        # Send signature in COMMITs only
        if self._all_bls_latest_multi_sigs is not None:
            pre_prepare_params.append([val.as_list() for val in self._all_bls_latest_multi_sigs])
            self._all_bls_latest_multi_sigs = None

        return pre_prepare_params

    def update_prepare(self, prepare_params, ledger_id):
        # Send BLS signature in COMMITs only
        return prepare_params

    @measure_time(MetricsName.BLS_UPDATE_COMMIT_TIME)
    def update_commit(self, commit_params, pre_prepare: PrePrepare):
        ledger_id = pre_prepare.ledgerId
        state_root_hash = pre_prepare.stateRootHash

        if not self._can_process_ledger(ledger_id):
            return commit_params

        if not self._bls_bft.can_sign_bls():
            logger.debug("{}{} can not sign COMMIT {} for state {}: No BLS keys"
                         .format(BLS_PREFIX, self, commit_params, state_root_hash))
            return commit_params

        bls_signature = self._sign_state(pre_prepare)
        logger.debug("{}{} signed COMMIT {} for state {} with sig {}"
                     .format(BLS_PREFIX, self, commit_params, state_root_hash, bls_signature))
        commit_params.append(bls_signature)

        last_audit_txn = self._get_correct_audit_transaction(pre_prepare)
        if last_audit_txn:
            res = {}
            payload_data = last_audit_txn[TXN_PAYLOAD][TXN_PAYLOAD_DATA]
            for ledger_id in payload_data[AUDIT_TXN_STATE_ROOT].keys():
                fake_pp = BlsBftReplicaPlenum._create_fake_pre_prepare_for_multi_sig(
                    ledger_id,
                    payload_data[AUDIT_TXN_STATE_ROOT].get(ledger_id),
                    payload_data[AUDIT_TXN_LEDGER_ROOT].get(ledger_id),
                    pre_prepare
                )
                bls_signature = self._sign_state(fake_pp)
                res[str(ledger_id)] = bls_signature
            commit_params.append(res)

        return commit_params

    # ----PROCESS----

    def process_pre_prepare(self, pre_prepare: PrePrepare, sender):
        # does not matter which ledger id is current PPR for
        # mult-sig is for domain ledger anyway
        self._save_multi_sig_shared(pre_prepare)

    def process_prepare(self, prepare: Prepare, sender):
        pass

    def process_commit(self, commit: Commit, sender):
        key_3PC = (commit.viewNo, commit.ppSeqNo)
        if f.BLS_SIG.nm in commit and commit.blsSig is not None:
            if key_3PC not in self._signatures:
                self._signatures[key_3PC] = {}
            self._signatures[key_3PC][self.get_node_name(sender)] = commit.blsSig

        if f.BLS_SIGS.nm in commit and commit.blsSigs is not None:
            if key_3PC not in self._all_signatures:
                self._all_signatures[key_3PC] = {}
            for ledger_id in commit.blsSigs.keys():
                if ledger_id not in self._all_signatures[key_3PC]:
                    self._all_signatures[key_3PC][ledger_id] = {}
                self._all_signatures[key_3PC][ledger_id][self.get_node_name(sender)] = commit.blsSigs[ledger_id]

    def process_order(self, key, quorums, pre_prepare):
        if not self._can_process_ledger(pre_prepare.ledgerId):
            return

        if not self._can_calculate_multi_sig(key, quorums):
            return

        # calculate signature always to keep master and non-master in sync
        # but save on master only
        bls_multi_sig = self._calculate_multi_sig(key, pre_prepare)

        all_bls_multi_sigs = self._calculate_all_multi_sigs(key, pre_prepare)

        if not self._is_master:
            return

        if all_bls_multi_sigs:
            for bls_multi_sig in all_bls_multi_sigs:
                self._save_multi_sig_local(bls_multi_sig)
        elif bls_multi_sig:
            self._save_multi_sig_local(bls_multi_sig)

        self._bls_latest_multi_sig = bls_multi_sig
        self._all_bls_latest_multi_sigs = all_bls_multi_sigs

    # ----GC----

    def gc(self, key_3PC):
        keys_to_remove = []
        for key in self._signatures.keys():
            if compare_3PC_keys(key, key_3PC) >= 0:
                keys_to_remove.append(key)
        for key in keys_to_remove:
            self._signatures.pop(key, None)
            self._all_signatures.pop(key, None)

    # ----MULT_SIG----

    def _create_multi_sig_value_for_pre_prepare(self, pre_prepare: PrePrepare, pool_state_root_hash):
        multi_sig_value = MultiSignatureValue(ledger_id=pre_prepare.ledgerId,
                                              state_root_hash=pre_prepare.stateRootHash,
                                              pool_state_root_hash=pool_state_root_hash,
                                              txn_root_hash=pre_prepare.txnRootHash,
                                              timestamp=pre_prepare.ppTime)
        return multi_sig_value

    def validate_key_proof_of_possession(self, key_proof, pk):
        return self._bls_bft.bls_crypto_verifier \
            .verify_key_proof_of_possession(key_proof, pk)

    def _validate_signature(self, sender, bls_sig, pre_prepare: PrePrepare):
        pool_root_hash = self._get_pool_root_hash(pre_prepare, serialize=False)
        sender_node = self.get_node_name(sender)
        pk = self._bls_bft.bls_key_register.get_key_by_name(sender_node, pool_root_hash)
        if not pk:
            return False
        pool_root_hash_ser = self._get_pool_root_hash(pre_prepare)
        message = self._create_multi_sig_value_for_pre_prepare(pre_prepare,
                                                               pool_root_hash_ser)
        result = self._bls_bft.bls_crypto_verifier.verify_sig(bls_sig, message.as_single_value(), pk)
        if not result:
            logger.info("Incorrect bls signature {} in commit for "
                        "{} public key: '{}' and message: '{}' from "
                        "pre-prepare: {}".format(bls_sig, sender, pk,
                                                 message, pre_prepare))
        return result

    def _validate_multi_sig(self, multi_sig: MultiSignature):
        public_keys = []
        pool_root_hash = self.state_root_serializer.deserialize(
            multi_sig.value.pool_state_root_hash)
        for node_name in multi_sig.participants:
            bls_key = self._bls_bft.bls_key_register.get_key_by_name(node_name,
                                                                     pool_root_hash)
            # TODO: It's optional for now
            if bls_key:
                public_keys.append(bls_key)
        value = multi_sig.value.as_single_value()
        return self._bls_bft.bls_crypto_verifier.verify_multi_sig(multi_sig.signature,
                                                                  value,
                                                                  public_keys)

    def _sign_state(self, pre_prepare: PrePrepare):
        pool_root_hash = self._get_pool_root_hash(pre_prepare)
        message = self._create_multi_sig_value_for_pre_prepare(pre_prepare,
                                                               pool_root_hash).as_single_value()
        return self._bls_bft.bls_crypto_signer.sign(message)

    def _can_calculate_multi_sig(self,
                                 key_3PC,
                                 quorums) -> bool:
        if key_3PC in self._all_signatures:
            sigs_for_request = self._all_signatures[key_3PC]
            sigs_invalid = list(
                filter(
                    lambda item: not quorums.bls_signatures.is_reached(len(list(item[1].values()))),
                    sigs_for_request.items()
                )
            )
            if sigs_invalid:
                for lid, sigs in sigs_invalid:
                    logger.debug(
                        '{}Can not create bls signatures for batch {}: '
                        'There are only {} signatures for ledger {}, '
                        'while {} required for multi_signature'.format(BLS_PREFIX,
                                                                       key_3PC,
                                                                       len(list(sigs.values())),
                                                                       quorums.bls_signatures.value,
                                                                       lid)
                    )
            else:
                return True

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

    def _calculate_multi_sig(self, key_3PC, pre_prepare) -> Optional[MultiSignature]:
        sigs_for_request = self._signatures[key_3PC]
        return self._calculate_single_multi_sig(sigs_for_request, pre_prepare)

    def _calculate_all_multi_sigs(self, key_3PC, pre_prepare) -> Optional[list]:
        sigs_for_request = self._all_signatures.get(key_3PC)
        res = []
        if sigs_for_request:
            for lid in sigs_for_request:
                sig = sigs_for_request[lid]
                audit_txn = self._get_correct_audit_transaction(pre_prepare)
                if audit_txn:
                    audit_payload = audit_txn[TXN_PAYLOAD][TXN_PAYLOAD_DATA]
                    fake_pp = BlsBftReplicaPlenum. \
                        _create_fake_pre_prepare_for_multi_sig(int(lid),
                                                               audit_payload[AUDIT_TXN_STATE_ROOT][int(lid)],
                                                               audit_payload[AUDIT_TXN_LEDGER_ROOT][int(lid)],
                                                               pre_prepare)
                    res.append(self._calculate_single_multi_sig(sig, fake_pp))
        return res

    def _calculate_single_multi_sig(self, sigs_for_request, pre_prepare) -> Optional[MultiSignature]:
        bls_signatures = list(sigs_for_request.values())
        participants = list(sigs_for_request.keys())

        sig = self._bls_bft.bls_crypto_verifier.create_multi_sig(bls_signatures)
        pool_root_hash_ser = self._get_pool_root_hash(pre_prepare)
        multi_sig_value = self._create_multi_sig_value_for_pre_prepare(pre_prepare,
                                                                       pool_root_hash_ser)
        return MultiSignature(signature=sig,
                              participants=participants,
                              value=multi_sig_value)

    def _get_pool_root_hash(self, pre_prepare, serialize=True):
        if f.POOL_STATE_ROOT_HASH.nm in pre_prepare:
            pool_root_hash = self.state_root_serializer.deserialize(pre_prepare.poolStateRootHash)
            pool_root_hash_ser = pre_prepare.poolStateRootHash
        else:
            pool_root_hash = self._bls_bft.bls_key_register.get_pool_root_hash_committed()
            pool_root_hash_ser = self.state_root_serializer.serialize(bytes(pool_root_hash))
        return pool_root_hash_ser if serialize else pool_root_hash

    def _save_multi_sig_local(self,
                              multi_sig: MultiSignature):
        self._bls_bft.bls_store.put(multi_sig)
        logger.debug("{}{} saved multi signature {} for root {} (locally calculated)"
                     .format(BLS_PREFIX, self, multi_sig,
                             multi_sig.value.state_root_hash))

    def _save_multi_sig_shared(self, pre_prepare: PrePrepare):

        if f.BLS_MULTI_SIGS.nm in pre_prepare and pre_prepare.blsMultiSigs is not None:
            multi_sigs = pre_prepare.blsMultiSigs
            for sig in multi_sigs:
                multi_sig = MultiSignature.from_list(*sig)
                self._bls_bft.bls_store.put(multi_sig)
                logger.debug("{}{} saved multi signature {} for root {} (calculated by Primary)"
                             .format(BLS_PREFIX, self, multi_sig,
                                     multi_sig.value.state_root_hash))
            return
        elif f.BLS_MULTI_SIG.nm not in pre_prepare:
            return
        if pre_prepare.blsMultiSig is None:
            return

        multi_sig = MultiSignature.from_list(*pre_prepare.blsMultiSig)
        self._bls_bft.bls_store.put(multi_sig)
        logger.debug("{}{} saved multi signature {} for root {} (calculated by Primary)"
                     .format(BLS_PREFIX, self, multi_sig,
                             multi_sig.value.state_root_hash))
        # TODO: support multiple multi-sigs for multiple previous batches

    def _get_correct_audit_transaction(self, pp: PrePrepare):
        ledger = self._database_manager.get_ledger(AUDIT_LEDGER_ID)
        if ledger is None:
            return None
        seqNo = ledger.uncommitted_size
        for curSeqNo in reversed(range(1, seqNo + 1)):
            txn = ledger.get_by_seq_no_uncommitted(curSeqNo)
            if txn:
                payload = txn[TXN_PAYLOAD][TXN_PAYLOAD_DATA]
                if pp.ppSeqNo == payload[AUDIT_TXN_PP_SEQ_NO]:
                    return txn
        return None

    @staticmethod
    def _create_fake_pre_prepare_for_multi_sig(lid, state_root_hash, txn_root_hash, pre_prepare):
        params = [
            pre_prepare.instId,
            pre_prepare.viewNo,
            pre_prepare.ppSeqNo,
            pre_prepare.ppTime,
            pre_prepare.reqIdr,
            pre_prepare.discarded,
            pre_prepare.digest,
            1,  # doing it to work around the ledgers that are not in plenum -- it will fail the validation of pre-prepare
            state_root_hash,
            txn_root_hash,
            pre_prepare.sub_seq_no,
            pre_prepare.final,
            pre_prepare.poolStateRootHash,
            pre_prepare.auditTxnRootHash
        ]
        pp = PrePrepare(*params)
        pp.ledgerId = lid
        return pp

    @staticmethod
    def get_node_name(replica_name: str):
        # TODO: there is the same method in Replica
        # It should be moved to some util class
        return replica_name.split(":")[0]

    def __str__(self, *args, **kwargs):
        return self.node_id
