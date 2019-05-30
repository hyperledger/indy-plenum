from functools import lru_cache

from common.serializers.serialization import pool_state_serializer
from plenum.common.constants import POOL_LEDGER_ID, NODE, DATA, BLS_KEY, \
    BLS_KEY_PROOF, TARGET_NYM, DOMAIN_LEDGER_ID, NODE_IP, \
    NODE_PORT, CLIENT_IP, CLIENT_PORT, ALIAS
from plenum.common.exceptions import InvalidClientRequest, UnauthorizedClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import get_payload_data, get_from
from plenum.common.types import f
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler
from plenum.server.request_handlers.utils import is_steward


class NodeHandler(WriteRequestHandler):
    state_serializer = pool_state_serializer

    def __init__(self, database_manager: DatabaseManager, bls_crypto_verifier):
        super().__init__(database_manager, NODE, POOL_LEDGER_ID)
        self.bls_crypto_verifier = bls_crypto_verifier

    def static_validation(self, request: Request):
        self._validate_request_type(request)
        blskey = request.operation.get(DATA).get(BLS_KEY, None)
        blskey_proof = request.operation.get(DATA).get(BLS_KEY_PROOF, None)
        if blskey is None and blskey_proof is None:
            return
        if blskey is None and blskey_proof is not None:
            raise InvalidClientRequest(request.identifier, request.reqId,
                                       "A Proof of possession is not "
                                       "needed without BLS key")
        if blskey is not None and blskey_proof is None:
            raise InvalidClientRequest(request.identifier, request.reqId,
                                       "A Proof of possession must be "
                                       "provided with BLS key")
        if not self._verify_bls_key_proof_of_possession(blskey_proof,
                                                        blskey):
            raise InvalidClientRequest(request.identifier, request.reqId,
                                       "Proof of possession {} is incorrect "
                                       "for BLS key {}".
                                       format(blskey_proof, blskey))

    def gen_state_key(self, txn):
        node_nym = get_payload_data(txn).get(TARGET_NYM)
        return node_nym.encode()

    def dynamic_validation(self, request: Request):
        self._validate_request_type(request)
        node_nym = request.operation.get(TARGET_NYM)
        if self._get_node_data(node_nym, is_committed=False):
            error = self._auth_error_while_updating_node(request)
        else:
            error = self._auth_error_while_adding_node(request)
        if error:
            raise UnauthorizedClientRequest(request.identifier, request.reqId,
                                            error)

    def update_state(self, txn, prev_result, is_committed=False):
        self._validate_txn_type(txn)
        node_nym = get_payload_data(txn).get(TARGET_NYM)
        data = get_payload_data(txn).get(DATA, {})
        existing_data = self._get_node_data(node_nym, is_committed=is_committed)
        # Node data did not exist in state, so this is a new node txn,
        # hence store the author of the txn (steward of node)
        if not existing_data:
            existing_data[f.IDENTIFIER.nm] = get_from(txn)
        existing_data.update(data)
        key = self.gen_state_key(txn)
        val = self.state_serializer.serialize(data)
        self.state.set(key, val)

    def _auth_error_while_adding_node(self, request):
        origin = request.identifier
        operation = request.operation
        data = operation.get(DATA, {})
        error = self._data_error_while_validating(data, skip_keys=False)
        if error:
            return error

        if self._steward_has_node(origin):
            return "{} already has a node".format(origin)
        error = self._is_node_data_conflicting(data)
        if error:
            return "existing data has conflicts with " \
                   "request data {}. Error: {}".format(operation.get(DATA), error)

    def _auth_error_while_updating_node(self, request):
        # Check if steward of the node is updating it and its data does not
        # conflict with any existing node's data
        origin = request.identifier
        operation = request.operation
        node_nym = operation.get(TARGET_NYM)
        if not self._is_steward_of_node(origin, node_nym, is_committed=False):
            return "{} is not a steward of node {}".format(origin, node_nym)

        data = operation.get(DATA, {})
        return self._data_error_while_validating_update(data, node_nym)

    def _get_node_data(self, nym, is_committed: bool = True):
        key = nym.encode()
        data = self.state.get(key, is_committed)
        if not data:
            return {}
        return self.state_serializer.deserialize(data)

    def get_all_node_data_for_root_hash(self, root_hash):
        leaves = self.state.get_all_leaves_for_root_hash(root_hash)
        raw_node_data = leaves.values()
        nodes = list(map(lambda x: self.state_serializer.deserialize(
            self.state.get_decoded(x)), raw_node_data))
        return nodes

    def _is_steward(self, nym, is_committed: bool = True):
        domain_state = self.database_manager.get_database(DOMAIN_LEDGER_ID).state
        return is_steward(domain_state, nym, is_committed)

    @lru_cache(maxsize=64)
    def _is_steward_of_node(self, steward_nym, node_nym, is_committed=True):
        node_data = self._get_node_data(node_nym, is_committed=is_committed)
        return node_data and node_data[f.IDENTIFIER.nm] == steward_nym

    def _steward_has_node(self, steward_nym) -> bool:
        # Cannot use lru_cache since a steward might have a node in future and
        # unfortunately lru_cache does not allow single entries to be cleared
        # TODO: Modify lru_cache to clear certain entities
        for nodeNym, nodeData in self.state.as_dict.items():
            nodeData = self.state_serializer.deserialize(nodeData)
            if nodeData.get(f.IDENTIFIER.nm) == steward_nym:
                return True
        return False

    @staticmethod
    def _data_error_while_validating(data, skip_keys):
        req_keys = {NODE_IP, NODE_PORT, CLIENT_IP, CLIENT_PORT, ALIAS}
        if not skip_keys and not req_keys.issubset(set(data.keys())):
            return 'Missing some of {}'.format(req_keys)

        nip = data.get(NODE_IP, 'nip')
        np = data.get(NODE_PORT, 'np')
        cip = data.get(CLIENT_IP, 'cip')
        cp = data.get(CLIENT_PORT, 'cp')
        if (nip, np) == (cip, cp):
            return 'node and client ha cannot be same'

    def _is_node_data_same(self, node_nym, new_data, is_committed=True):
        node_info = self._get_node_data(node_nym, is_committed=is_committed)
        node_info.pop(f.IDENTIFIER.nm, None)
        return node_info == new_data

    def _is_node_data_conflicting(self, new_data, updating_nym=None):
        # Check if node's ALIAS or IPs or ports conflicts with other nodes,
        # also, the node is not allowed to change its alias.

        # Check ALIAS change
        if updating_nym:
            old_alias = self._get_node_data(updating_nym, is_committed=False).get(ALIAS)
            new_alias = new_data.get(ALIAS)
            if old_alias != new_alias:
                return "Node's alias cannot be changed"

        nodes = self.state.as_dict.items()
        for other_node_nym, other_node_data in nodes:
            other_node_nym = other_node_nym.decode()
            other_node_data = self.state_serializer.deserialize(other_node_data)
            if not updating_nym or other_node_nym != updating_nym:
                # The node's ip, port and alias should be unique
                same_alias = new_data.get(ALIAS) == other_node_data.get(ALIAS)
                if same_alias:
                    return "Node's alias must be unique"
                same_node_ha = (new_data.get(NODE_IP), new_data.get(NODE_PORT)) == \
                               (other_node_data.get(NODE_IP), other_node_data.get(NODE_PORT))
                if same_node_ha:
                    return "Node's nodestack addresses must be unique"
                same_cli_ha = (new_data.get(CLIENT_IP), new_data.get(CLIENT_PORT)) == \
                              (other_node_data.get(CLIENT_IP), other_node_data.get(CLIENT_PORT))
                if same_cli_ha:
                    return "Node's clientstack addresses must be unique"

    def _data_error_while_validating_update(self, data, node_nym):
        error = self._data_error_while_validating(data, skip_keys=True)
        if error:
            return error

        if self._is_node_data_same(node_nym, data, is_committed=False):
            return "node already has the same data as requested"

        error = self._is_node_data_conflicting(data, node_nym)
        if error:
            return "existing data has conflicts with " \
                   "request data {}. Error: {}".format(data, error)

    def _verify_bls_key_proof_of_possession(self, key_proof, pk):
        return True if self.bls_crypto_verifier is None else \
            self.bls_crypto_verifier.verify_key_proof_of_possession(key_proof,
                                                                    pk)
