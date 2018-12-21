from common.serializers.serialization import state_roots_serializer, proof_nodes_serializer
from plenum.common.constants import ROOT_HASH, MULTI_SIGNATURE, PROOF_NODES, DOMAIN_LEDGER_ID
from plenum.common.request import Request
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.request_handler import RequestHandler


class ReadRequestHandler(RequestHandler):
    def __init__(self, node, database_manager: DatabaseManager, txn_type, ledger_id):
        self.node = node
        self.database_manager = database_manager
        self.txn_type = txn_type
        self.ledger_id = ledger_id

    def static_validation(self, request: Request):
        pass

    def dynamic_validation(self, request: Request):
        pass

    def get_result(self, request: Request):
        pass

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

    @property
    def state(self):
        return self.database_manager.get_database(self.ledger_id)

    @property
    def bls_store(self):
        return self.database_manager.get_store('bls')