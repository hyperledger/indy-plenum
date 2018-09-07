from binascii import hexlify
from hashlib import sha256

from common.serializers.serialization import domain_state_serializer, \
    proof_nodes_serializer, state_roots_serializer
from ledger.util import F
from plenum.common.constants import TXN_TYPE, NYM, ROLE, STEWARD, TARGET_NYM, \
    VERKEY, TXN_TIME, ROOT_HASH, MULTI_SIGNATURE, PROOF_NODES, DATA, \
    STATE_PROOF
from plenum.common.exceptions import UnauthorizedClientRequest
from plenum.common.plenum_protocol_version import PlenumProtocolVersion
from plenum.common.request import Request
from plenum.common.txn_util import reqToTxn, get_type, get_payload_data, get_seq_no, get_txn_time, get_from
from plenum.common.types import f
from plenum.server.ledger_req_handler import LedgerRequestHandler
from stp_core.common.log import getlogger

logger = getlogger()


class DomainRequestHandler(LedgerRequestHandler):
    stateSerializer = domain_state_serializer
    write_types = {NYM, }

    def __init__(self, ledger, state, config, reqProcessors, bls_store, ts_store=None):
        super().__init__(ledger, state, ts_store=ts_store)
        self.config = config
        self.reqProcessors = reqProcessors
        self.bls_store = bls_store

    def doStaticValidation(self, request: Request):
        pass

    def validate(self, req: Request):
        if req.operation.get(TXN_TYPE) == NYM:
            origin = req.identifier
            error = None
            if not self.isSteward(self.state,
                                  origin, isCommitted=False):
                error = "Only Steward is allowed to do these transactions"
            if req.operation.get(ROLE) == STEWARD:
                if self.stewardThresholdExceeded(self.config):
                    error = "New stewards cannot be added by other stewards " \
                            "as there are already {} stewards in the system".\
                            format(self.config.stewardThreshold)
            if error:
                raise UnauthorizedClientRequest(req.identifier,
                                                req.reqId,
                                                error)

    def _reqToTxn(self, req: Request):
        txn = reqToTxn(req)
        for processor in self.reqProcessors:
            res = processor.process(req)
            txn.update(res)
        return txn

    @staticmethod
    def transform_txn_for_ledger(txn):
        """
        Some transactions need to be updated before they can be stored in the
        ledger, eg. storing certain payload in another data store and only its
        hash in the ledger
        """
        return txn

    def updateState(self, txns, isCommitted=False):
        for txn in txns:
            self._updateStateWithSingleTxn(txn, isCommitted=isCommitted)

    def gen_txn_path(self, txn):
        typ = get_type(txn)
        if typ == NYM:
            nym = get_payload_data(txn).get(TARGET_NYM)
            return hexlify(self.nym_to_state_key(nym)).decode()
        else:
            logger.error('Cannot generate id for txn of type {}'.format(typ))
            return None

    def _updateStateWithSingleTxn(self, txn, isCommitted=False):
        typ = get_type(txn)
        if typ == NYM:
            nym = get_payload_data(txn).get(TARGET_NYM)
            self.updateNym(nym, txn, isCommitted=isCommitted)
        else:
            logger.debug(
                'Cannot apply request of type {} to state'.format(typ))

    def countStewards(self) -> int:
        """
        Count the number of stewards added to the pool transaction store
        Note: This is inefficient, a production use case of this function
        should require an efficient storage mechanism
        """
        # THIS SHOULD NOT BE DONE FOR PRODUCTION
        return sum(1 for _, txn in self.ledger.getAllTxn() if
                   (get_type(txn) == NYM) and (get_payload_data(txn).get(ROLE) == STEWARD))

    def stewardThresholdExceeded(self, config) -> bool:
        """We allow at most `stewardThreshold` number of  stewards to be added
        by other stewards"""
        return self.countStewards() > config.stewardThreshold

    def updateNym(self, nym, txn, isCommitted=True):
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
        existingData.update(newData)
        val = self.stateSerializer.serialize(existingData)
        key = self.nym_to_state_key(nym)
        self.state.set(key, val)
        return existingData

    def hasNym(self, nym, isCommitted: bool=True):
        key = self.nym_to_state_key(nym)
        data = self.state.get(key, isCommitted)
        return bool(data)

    @staticmethod
    def get_role(state, nym, role, isCommitted: bool=True):
        nym_data = DomainRequestHandler.getNymDetails(state, nym, isCommitted)
        if nym_data.get(ROLE) == role:
            return nym_data
        else:
            return {}

    @staticmethod
    def getSteward(state, nym, isCommitted: bool=True):
        return DomainRequestHandler.get_role(state, nym, STEWARD, isCommitted)

    @staticmethod
    def isSteward(state, nym, isCommitted: bool=True):
        return bool(DomainRequestHandler.getSteward(state,
                                                    nym,
                                                    isCommitted))

    @staticmethod
    def getNymDetails(state, nym, isCommitted: bool=True):
        key = DomainRequestHandler.nym_to_state_key(nym)
        data = state.get(key, isCommitted)
        if not data:
            return {}
        return DomainRequestHandler.stateSerializer.deserialize(data)

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

        multi_sig = self.bls_store.get(encoded_root_hash)
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
