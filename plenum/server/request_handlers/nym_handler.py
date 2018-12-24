from _sha256 import sha256
from binascii import hexlify

from common.exceptions import LogicError
from common.serializers.serialization import state_roots_serializer, \
    proof_nodes_serializer, domain_state_serializer
from ledger.util import F
from plenum.common.constants import NYM, ROLE, STEWARD, DOMAIN_LEDGER_ID, \
    ROOT_HASH, MULTI_SIGNATURE, PROOF_NODES, DATA, TXN_TIME, STATE_PROOF, \
    VERKEY, TARGET_NYM
from plenum.common.exceptions import UnauthorizedClientRequest
from plenum.common.plenum_protocol_version import PlenumProtocolVersion
from plenum.common.request import Request
from plenum.common.txn_util import get_type, get_payload_data, get_from, \
    get_seq_no, get_txn_time, get_request_data
from plenum.common.types import f
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler
from stp_core.common.log import getlogger

logger = getlogger()


class NymHandler(WriteRequestHandler):
    stateSerializer = domain_state_serializer

    def __init__(self, config, database_manager: DatabaseManager):
        super().__init__(database_manager, NYM, DOMAIN_LEDGER_ID)
        self.config = config
        self._steward_count = 0

    def static_validation(self, request: Request):
        pass

    def dynamic_validation(self, request: Request):
        self._validate_type(request)
        identifier, req_id, operation = get_request_data(request)
        error = None
        if not self.isSteward(self.state,
                              identifier, isCommitted=False):
            error = "Only Steward is allowed to do these transactions"
        if operation.get(ROLE) == STEWARD:
            if self.steward_threshold_exceeded(self.config):
                error = "New stewards cannot be added by other stewards " \
                        "as there are already {} stewards in the system". \
                    format(self.config.stewardThreshold)
        if error:
            raise UnauthorizedClientRequest(identifier,
                                            req_id,
                                            error)

    @staticmethod
    def transform_txn_for_ledger(txn):
        """
        Some transactions need to be updated before they can be stored in the
        ledger, eg. storing certain payload in another data store and only its
        hash in the ledger
        """
        return txn

    def update_state(self, txns, isCommitted=False):
        for txn in txns:
            self._update_state_with_single_txn(txn, isCommitted=isCommitted)

    def gen_txn_path(self, txn):
        typ = get_type(txn)
        if typ == NYM:
            nym = get_payload_data(txn).get(TARGET_NYM)
            return hexlify(self.nym_to_state_key(nym)).decode()
        else:
            raise LogicError

    def _update_state_with_single_txn(self, txn, isCommitted=False):
        typ = get_type(txn)
        if typ == NYM:
            nym = get_payload_data(txn).get(TARGET_NYM)
            self.update_nym(nym, txn, isCommitted=isCommitted)
        else:
            raise LogicError

    def count_stewards(self) -> int:
        """
        Count the number of stewards added to the pool transaction store
        Note: This is inefficient, a production use case of this function
        should require an efficient storage mechanism
        """
        return self._steward_count

    def steward_threshold_exceeded(self, config) -> bool:
        """We allow at most `stewardThreshold` number of  stewards to be added
        by other stewards"""
        return self.count_stewards() >= config.stewardThreshold

    def update_nym(self, nym, txn, isCommitted=True):
        existingData = self.getNymDetails(self.state, nym,
                                          isCommitted=isCommitted)
        txn_data = get_payload_data(txn)
        newData = {}
        if not existingData:
            # New nym being added to state, set the TrustAnchor
            newData[f.IDENTIFIER.nm] = get_from(txn)
            # New nym being added to state, set the role and verkey to None, this makes
            # the state data always have a value for `role` and `verkey` since we allow
            # clients to omit specifying `role` and `verkey` in the request consider a
            # default value of None
            newData[ROLE] = None
            newData[VERKEY] = None

        if ROLE in txn_data:
            newData[ROLE] = txn_data[ROLE]
        if VERKEY in txn_data:
            newData[VERKEY] = txn_data[VERKEY]
        newData[F.seqNo.name] = get_seq_no(txn)
        newData[TXN_TIME] = get_txn_time(txn)
        self._update_steward_count(newData, existingData)
        existingData.update(newData)
        val = self.stateSerializer.serialize(existingData)
        key = self.nym_to_state_key(nym)
        self.state.set(key, val)
        return existingData

    def has_nym(self, nym, isCommitted: bool = True):
        key = self.nym_to_state_key(nym)
        data = self.state.get(key, isCommitted)
        return bool(data)

    @staticmethod
    def get_role(state, nym, isCommitted: bool = True):
        nymData = NymHandler.getNymDetails(state, nym, isCommitted)
        if not nymData:
            return {}
        else:
            return nymData.get(ROLE)

    @staticmethod
    def isSteward(state, nym, isCommitted: bool = True):
        role = NymHandler.get_role(state, nym, isCommitted)
        return role is STEWARD

    @staticmethod
    def getNymDetails(state, nym, isCommitted: bool = True):
        key = NymHandler.nym_to_state_key(nym)
        data = state.get(key, isCommitted)
        if not data:
            return {}
        return NymHandler.stateSerializer.deserialize(data)

    @staticmethod
    def nym_to_state_key(nym: str) -> bytes:
        return sha256(nym.encode()).digest()

    def get_value_from_state(self, path, head_hash=None, with_proof=False):
        '''
        Get a value (and proof optionally)for the given path in state trie.
        Does not return the proof is there is no aggregate signature for it.
        :param path: the path generate a state proof for
        :param head_hash: the root to create the proof against
        :param get_value: whether to return the value
        :return: a state proof or None
        '''
        root_hash = head_hash if head_hash else self.state.committedHeadHash
        encoded_root_hash = state_roots_serializer.serialize(bytes(root_hash))

        if not with_proof:
            return self.state.get_for_root_hash(root_hash, path), None

        multi_sig = self.database_manager.bls_store.get(encoded_root_hash)
        if not multi_sig:
            # Just return the value and not proof
            try:
                return self.state.get_for_root_hash(root_hash, path), None
            except KeyError:
                return None, None
        else:
            try:
                proof, value = self.state.generate_state_proof(key=path,
                                                               root=self.state.get_head_by_hash(root_hash),
                                                               serialize=True,
                                                               get_value=True)
                value = self.state.get_decoded(value) if value else value
                encoded_proof = proof_nodes_serializer.serialize(proof)
                proof = {
                    ROOT_HASH: encoded_root_hash,
                    MULTI_SIGNATURE: multi_sig.as_dict(),
                    PROOF_NODES: encoded_proof
                }
                return value, proof
            except KeyError:
                return None, None

    @staticmethod
    def make_result(request, data, last_seq_no, update_time, proof):
        result = {**request.operation, **{
            DATA: data,
            f.IDENTIFIER.nm: request.identifier,
            f.REQ_ID.nm: request.reqId,
            f.SEQ_NO.nm: last_seq_no,
            TXN_TIME: update_time
        }}
        if proof and request.protocolVersion and \
                request.protocolVersion >= PlenumProtocolVersion.STATE_PROOF_SUPPORT.value:
            result[STATE_PROOF] = proof

        # Do not inline please, it makes debugging easier
        return result

    def _update_steward_count(self, new_data, existing_data=None):
        if not existing_data:
            existing_data = {}
            existing_data.setdefault(ROLE, "")
        if existing_data[ROLE] == STEWARD and new_data[ROLE] != STEWARD:
            self._steward_count -= 1
        elif existing_data[ROLE] != STEWARD and new_data[ROLE] == STEWARD:
            self._steward_count += 1
