from abc import abstractmethod

from common.serializers.serialization import state_roots_serializer, proof_nodes_serializer
from plenum.common.constants import ROOT_HASH, MULTI_SIGNATURE, PROOF_NODES
from plenum.common.request import Request
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.request_handler import RequestHandler


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
