from functools import lru_cache

from common.serializers.serialization import pool_state_serializer
from plenum.common.constants import TXN_TYPE, NODE, TARGET_NYM, DATA, ALIAS, \
    NODE_IP, NODE_PORT, CLIENT_IP, CLIENT_PORT, SERVICES
from plenum.common.exceptions import UnauthorizedClientRequest
from plenum.common.ledger import Ledger
from plenum.common.request import Request
from plenum.common.txn_util import reqToTxn
from plenum.common.types import f
from plenum.persistence.util import txnsWithSeqNo
from plenum.server.domain_req_handler import DomainRequestHandler
from plenum.server.req_handler import RequestHandler
from state.state import State
from stp_core.common.log import getlogger

logger = getlogger()


class PoolRequestHandler(RequestHandler):
    write_types = {NODE, }

    def __init__(self, ledger: Ledger, state: State,
                 domainState: State):
        super().__init__(ledger, state)
        self.domainState = domainState
        self.stateSerializer = pool_state_serializer

    def doStaticValidation(self, request: Request):
        pass

    def validate(self, req: Request, config=None):
        typ = req.operation.get(TXN_TYPE)
        error = None
        if typ == NODE:
            nodeNym = req.operation.get(TARGET_NYM)
            if self.getNodeData(nodeNym, isCommitted=False):
                error = self.authErrorWhileUpdatingNode(req)
            else:
                error = self.authErrorWhileAddingNode(req)
        if error:
            raise UnauthorizedClientRequest(req.identifier, req.reqId,
                                            error)

    def apply(self, req: Request, cons_time: int):
        typ = req.operation.get(TXN_TYPE)
        if typ == NODE:
            txn = reqToTxn(req, cons_time)
            (start, end), _ = self.ledger.appendTxns([txn])
            self.updateState(txnsWithSeqNo(start, end, [txn]))
            return start, txn
        else:
            logger.debug(
                'Cannot apply request of type {} to state'.format(typ))
            return None

    def updateState(self, txns, isCommitted=False):
        for txn in txns:
            nodeNym = txn.get(TARGET_NYM)
            data = txn.get(DATA, {})
            existingData = self.getNodeData(nodeNym, isCommitted=isCommitted)
            # Node data did not exist in state, so this is a new node txn,
            # hence store the author of the txn (steward of node)
            if not existingData:
                existingData[f.IDENTIFIER.nm] = txn.get(f.IDENTIFIER.nm)
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
        if self.isNodeDataConflicting(data):
            return "existing data has conflicts with " \
                   "request data {}".format(operation.get(DATA))

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
        return self.stateSerializer.deserialize(data)

    def get_node_data_for_root_hash(self, root_hash, nym):
        key = nym.encode()
        data = self.state.get_for_root_hash(root_hash, key)
        if not data:
            return {}
        return self.stateSerializer.deserialize(data)

    def updateNodeData(self, nym, data):
        key = nym.encode()
        val = self.stateSerializer.serialize(data)
        self.state.set(key, val)

    def isSteward(self, nym, isCommitted: bool = True):
        return DomainRequestHandler.isSteward(
            self.domainState, nym, isCommitted)

    @lru_cache(maxsize=64)
    def isStewardOfNode(self, stewardNym, nodeNym, isCommitted=True):
        nodeData = self.getNodeData(nodeNym, isCommitted=isCommitted)
        return nodeData and nodeData[f.IDENTIFIER.nm] == stewardNym

    def stewardHasNode(self, stewardNym) -> bool:
        # Cannot use lru_cache since a steward might have a node in future and
        # unfortunately lru_cache does not allow single entries to be cleared
        # TODO: Modify lru_cache to clear certain entities
        for nodeNym, nodeData in self.state.as_dict.items():
            nodeData = self.stateSerializer.deserialize(nodeData)
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

    def isNodeDataConflicting(self, data, updatingNym=None):
        # Check if node's ALIAS or IPs or ports conflicts with other nodes,
        # also, the node is not allowed to change its alias.

        # Check ALIAS change
        nodeData = {}
        if updatingNym:
            nodeData = self.getNodeData(updatingNym, isCommitted=False)
            if nodeData.get(ALIAS) != data.get(ALIAS):
                return True
            else:
                # Preparing node data for check coming next
                nodeData.pop(f.IDENTIFIER.nm, None)
                nodeData.pop(SERVICES, None)
                nodeData.update(data)

        for otherNode, otherNodeData in self.state.as_dict.items():
            otherNode = otherNode.decode()
            otherNodeData = self.stateSerializer.deserialize(otherNodeData)
            otherNodeData.pop(f.IDENTIFIER.nm, None)
            otherNodeData.pop(SERVICES, None)
            if not updatingNym or otherNode != updatingNym:
                # The node's ip, port and alias shuuld be unique
                bag = set()
                for d in (nodeData, otherNodeData):
                    bag.add(d.get(ALIAS))
                    bag.add((d.get(NODE_IP), d.get(NODE_PORT)))
                    bag.add((d.get(CLIENT_IP), d.get(CLIENT_PORT)))

                list(map(lambda x: bag.remove(x) if x in bag else None,
                         (None, (None, None))))

                if (not nodeData and len(bag) != 3) or (
                        nodeData and len(bag) != 6):
                    return True
            if data.get(ALIAS) == otherNodeData.get(
                    ALIAS) and not updatingNym:
                return True

    def dataErrorWhileValidatingUpdate(self, data, nodeNym):
        error = self.dataErrorWhileValidating(data, skipKeys=True)
        if error:
            return error

        if self.isNodeDataSame(nodeNym, data, isCommitted=False):
            return "node already has the same data as requested"

        if self.isNodeDataConflicting(data, nodeNym):
            return "existing data has conflicts with " \
                   "request data {}".format(data)
