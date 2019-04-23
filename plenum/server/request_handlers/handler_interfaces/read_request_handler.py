from abc import abstractmethod

from common.serializers.serialization import state_roots_serializer, proof_nodes_serializer
from plenum.common.constants import ROOT_HASH, MULTI_SIGNATURE, PROOF_NODES, DATA, TXN_TIME, STATE_PROOF
from plenum.common.plenum_protocol_version import PlenumProtocolVersion
from plenum.common.request import Request
from plenum.common.types import f
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.request_handler import RequestHandler
from plenum.server.request_handlers.utils import decode_state_value


class ReadRequestHandler(RequestHandler):
    def __init__(self, database_manager: DatabaseManager, txn_type, ledger_id):
        super().__init__(database_manager, txn_type, ledger_id)

    @abstractmethod
    def static_validation(self, request: Request):
        pass

    @abstractmethod
    def dynamic_validation(self, request: Request):
        pass

    @abstractmethod
    def get_result(self, request: Request):
        pass

    def _get_value_from_state(self, path, head_hash=None, with_proof=False):
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

    def lookup(self, path, is_committed=True, with_proof=False) -> (str, int):
        """
        Queries state for data on specified path

        :param path: path to data
        :param is_committed: queries the committed state root if True else the uncommitted root
        :param with_proof: creates proof if True
        :return: data
        """
        assert path is not None
        head_hash = self.state.committedHeadHash if is_committed else self.state.headHash
        encoded, proof = self._get_value_from_state(path, head_hash, with_proof=with_proof)
        if encoded:
            value, last_seq_no, last_update_time = decode_state_value(encoded)
            return value, last_seq_no, last_update_time, proof
        return None, None, None, proof

    @staticmethod
    def make_result(request, data, last_seq_no=None, update_time=None, proof=None):

        result = {**request.operation, **{
            DATA: data,
            f.IDENTIFIER.nm: request.identifier,
            f.REQ_ID.nm: request.reqId
        }}
        if last_seq_no:
            result[f.SEQ_NO.nm] = last_seq_no
        if update_time:
            result[TXN_TIME] = update_time
        if proof and request.protocolVersion and \
                request.protocolVersion >= PlenumProtocolVersion.STATE_PROOF_SUPPORT.value:
            result[STATE_PROOF] = proof

        # Do not inline please, it makes debugging easier
        return result
