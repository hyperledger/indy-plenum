from functools import lru_cache

from common.exceptions import LogicError
from common.serializers.serialization import pool_state_serializer
from plenum.common.constants import POOL_LEDGER_ID, NODE, DATA, BLS_KEY, BLS_KEY_PROOF, TXN_TYPE, TARGET_NYM, \
    DOMAIN_LEDGER_ID, NODE_IP, NODE_PORT, CLIENT_IP, CLIENT_PORT, ALIAS
from plenum.common.exceptions import InvalidClientRequest, UnauthorizedClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import get_payload_data, get_from
from plenum.common.types import f
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler
from plenum.server.request_handlers.nym_handler import NymHandler


class NodeHandler(WriteRequestHandler):

    def __init__(self, database_manager: DatabaseManager, bls_crypto_verifier):
        super().__init__(database_manager, NODE, POOL_LEDGER_ID)
        self.bls_crypto_verifier = bls_crypto_verifier
        self.state_serializer = pool_state_serializer

    def static_validation(self, request: Request):
        if request.txn_type != NODE:
            raise LogicError
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

    def dynamic_validation(self, request: Request):
        typ = request.operation.get(TXN_TYPE)
        if typ != NODE:
            raise LogicError
        nodeNym = request.operation.get(TARGET_NYM)
        if self.getNodeData(nodeNym, isCommitted=False):
            error = self.authErrorWhileUpdatingNode(request)
        else:
            error = self.authErrorWhileAddingNode(request)
        if error:
            raise UnauthorizedClientRequest(request.identifier, request.reqId,
                                            error)

    def apply_request(self, request: Request, batch_ts):
        typ = request.operation.get(TXN_TYPE)
        if typ != NODE:
            raise LogicError
        return super().apply_request(request, batch_ts)

    def revert_request(self, request: Request, batch_ts):
        pass

    def updateState(self, txns, isCommitted=False):
        for txn in txns:
            nodeNym = get_payload_data(txn).get(TARGET_NYM)
            data = get_payload_data(txn).get(DATA, {})
            existingData = self.getNodeData(nodeNym, isCommitted=isCommitted)
            # Node data did not exist in state, so this is a new node txn,
            # hence store the author of the txn (steward of node)
            if not existingData:
                existingData[f.IDENTIFIER.nm] = get_from(txn)
            existingData.update(data)
            self.updateNodeData(nodeNym, existingData)

    def authErrorWhileAddingNode(self, request):
        origin = request.identifier
        operation = request.operation
        data = operation.get(DATA, {})
        error = self.dataErrorWhileValidating(data, skipKeys=False)
        if error:
            return error

        isSteward = self.isSteward(origin, isCommitted=False)
        if not isSteward:
            return "{} is not a steward so cannot add a new node".format(
                origin)
        if self.stewardHasNode(origin):
            return "{} already has a node".format(origin)
        error = self.isNodeDataConflicting(data)
        if error:
            return "existing data has conflicts with " \
                   "request data {}. Error: {}".format(operation.get(DATA), error)

    def authErrorWhileUpdatingNode(self, request):
        # Check if steward of the node is updating it and its data does not
        # conflict with any existing node's data
        origin = request.identifier
        operation = request.operation
        isSteward = self.isSteward(origin, isCommitted=False)
        if not isSteward:
            return "{} is not a steward so cannot update a node".format(origin)

        nodeNym = operation.get(TARGET_NYM)
        if not self.isStewardOfNode(origin, nodeNym, isCommitted=False):
            return "{} is not a steward of node {}".format(origin, nodeNym)

        data = operation.get(DATA, {})
        return self.dataErrorWhileValidatingUpdate(data, nodeNym)

    def getNodeData(self, nym, isCommitted: bool = True):
        key = nym.encode()
        data = self.state.get(key, isCommitted)
        if not data:
            return {}
        return self.state_serializer.deserialize(data)

    def get_node_data_for_root_hash(self, root_hash, nym):
        key = nym.encode()
        data = self.state.get_for_root_hash(root_hash, key)
        if not data:
            return {}
        return self.state_serializer.deserialize(data)

    def get_all_node_data_for_root_hash(self, root_hash):
        leaves = self.state.get_all_leaves_for_root_hash(root_hash)
        raw_node_data = leaves.values()
        nodes = list(map(lambda x: self.state_serializer.deserialize(
            self.state.get_decoded(x)), raw_node_data))
        return nodes

    def updateNodeData(self, nym, data):
        key = nym.encode()
        val = self.state_serializer.serialize(data)
        self.state.set(key, val)

    def isSteward(self, nym, isCommitted: bool = True):
        domain_state = self.database_manager.get_database(DOMAIN_LEDGER_ID).state
        return NymHandler.isSteward(
            domain_state, nym, isCommitted)

    @lru_cache(maxsize=64)
    def isStewardOfNode(self, stewardNym, nodeNym, isCommitted=True):
        nodeData = self.getNodeData(nodeNym, isCommitted=isCommitted)
        return nodeData and nodeData[f.IDENTIFIER.nm] == stewardNym

    def stewardHasNode(self, stewardNym) -> bool:
        # Cannot use lru_cache since a steward might have a node in future and
        # unfortunately lru_cache does not allow single entries to be cleared
        # TODO: Modify lru_cache to clear certain entities
        for nodeNym, nodeData in self.state.as_dict.items():
            nodeData = self.state_serializer.deserialize(nodeData)
            if nodeData.get(f.IDENTIFIER.nm) == stewardNym:
                return True
        return False

    @staticmethod
    def dataErrorWhileValidating(data, skipKeys):
        reqKeys = {NODE_IP, NODE_PORT, CLIENT_IP, CLIENT_PORT, ALIAS}
        if not skipKeys and not reqKeys.issubset(set(data.keys())):
            return 'Missing some of {}'.format(reqKeys)

        nip = data.get(NODE_IP, 'nip')
        np = data.get(NODE_PORT, 'np')
        cip = data.get(CLIENT_IP, 'cip')
        cp = data.get(CLIENT_PORT, 'cp')
        if (nip, np) == (cip, cp):
            return 'node and client ha cannot be same'

    def isNodeDataSame(self, nodeNym, newData, isCommitted=True):
        nodeInfo = self.getNodeData(nodeNym, isCommitted=isCommitted)
        nodeInfo.pop(f.IDENTIFIER.nm, None)
        return nodeInfo == newData

    def isNodeDataConflicting(self, new_data, updating_nym=None):
        # Check if node's ALIAS or IPs or ports conflicts with other nodes,
        # also, the node is not allowed to change its alias.

        # Check ALIAS change
        if updating_nym:
            old_alias = self.getNodeData(updating_nym, isCommitted=False).get(ALIAS)
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

    def dataErrorWhileValidatingUpdate(self, data, nodeNym):
        error = self.dataErrorWhileValidating(data, skipKeys=True)
        if error:
            return error

        if self.isNodeDataSame(nodeNym, data, isCommitted=False):
            return "node already has the same data as requested"

        error = self.isNodeDataConflicting(data, nodeNym)
        if error:
            return "existing data has conflicts with " \
                   "request data {}. Error: {}".format(data, error)

    def _verify_bls_key_proof_of_possession(self, key_proof, pk):
        return True if self.bls_crypto_verifier is None else \
            self.bls_crypto_verifier.verify_key_proof_of_possession(key_proof,
                                                                    pk)
