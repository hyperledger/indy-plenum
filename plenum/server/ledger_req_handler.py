from abc import ABCMeta, abstractmethod
from typing import List, Optional

from plenum.common.constants import STATE_PROOF, TXN_TIME, DATA, MULTI_SIGNATURE, PROOF_NODES, ROOT_HASH

from common.exceptions import PlenumValueError, LogicError
from common.serializers.serialization import state_roots_serializer, proof_nodes_serializer
from plenum.common.plenum_protocol_version import PlenumProtocolVersion
from plenum.common.types import f
from storage.state_ts_store import StateTsDbStorage
from stp_core.common.log import getlogger

from plenum.common.ledger import Ledger
from plenum.common.request import Request
from plenum.server.req_handler import RequestHandler
from plenum.common.txn_util import reqToTxn, append_txn_metadata

from state.state import State

logger = getlogger()


class LedgerRequestHandler(RequestHandler, metaclass=ABCMeta):
    """
    Base class for request handlers
    Declares methods for validation, application of requests and
    state control
    """

    query_types = set()
    write_types = set()

    def __init__(self, ledger_id: int, ledger: Ledger, state: State,
                 ts_store: Optional[StateTsDbStorage] = None):
        self.ledger_id = ledger_id
        self.ledger = ledger
        self.state = state
        self.ts_store = ts_store

    def updateState(self, txns, isCommitted=False):
        """
        Updates current state with a number of committed or
        not committed transactions
        """

    def gen_txn_path(self, txn):
        return None

    def _reqToTxn(self, req: Request):
        return reqToTxn(req)

    def apply(self, req: Request, cons_time: int):
        txn = self._reqToTxn(req)

        txn = append_txn_metadata(txn, txn_id=self.gen_txn_path(txn))

        self.ledger.append_txns_metadata([txn], cons_time)
        (start, end), _ = self.ledger.appendTxns(
            [self.transform_txn_for_ledger(txn)])
        self.updateState([txn])
        return start, txn

    def commit(self, txnCount, stateRoot, txnRoot, ppTime) -> List:
        """
        :param txnCount: The number of requests to commit (The actual requests
        are picked up from the uncommitted list from the ledger)
        :param stateRoot: The state trie root after the txns are committed
        :param txnRoot: The txn merkle root after the txns are committed

        :return: list of committed transactions
        """

        return self._commit(self.ledger_id, self.ledger, self.state, txnCount,
                            stateRoot, txnRoot, ppTime, ts_store=self.ts_store)

    def applyForced(self, req: Request):
        if not req.isForced():
            raise LogicError('requestHandler.applyForce method is called '
                             'for not forced request: {}'.format(req))

    def onBatchCreated(self, state_root, txn_time):
        pass

    def onBatchRejected(self):
        pass

    @abstractmethod
    def doStaticValidation(self, request: Request):
        pass

    def is_query(self, txn_type):
        return txn_type in self.query_types

    def get_query_response(self, request):
        raise NotImplementedError

    @staticmethod
    def transform_txn_for_ledger(txn):
        return txn

    @staticmethod
    def _commit(ledger_id, ledger, state, txnCount, stateRoot, txnRoot, ppTime, ts_store=None):
        _, committedTxns = ledger.commitTxns(txnCount)
        stateRoot = state_roots_serializer.deserialize(stateRoot.encode()) if isinstance(
            stateRoot, str) else stateRoot
        # TODO test for that
        if ledger.root_hash != txnRoot:
            # Probably the following fail should trigger catchup
            # TODO add repr / str for Ledger class and dump it here as well
            raise PlenumValueError(
                'txnRoot', txnRoot,
                ("equal to current ledger root hash {}"
                 .format(ledger.root_hash))
            )
        state.commit(rootHash=stateRoot)
        if ts_store:
            ts_store.set(ppTime, stateRoot, ledger_id)
        return committedTxns

    @property
    def operation_types(self) -> set:
        return self.write_types.union(self.query_types)

    @property
    def valid_txn_types(self) -> set:
        return self.write_types.union(self.query_types)

    def get_value_from_state(self, path, head_hash=None, with_proof=False, multi_sig=None):
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
    def make_result(request, data, proof=None):
        result = {**request.operation, **{
            DATA: data,
            f.IDENTIFIER.nm: request.identifier,
            f.REQ_ID.nm: request.reqId
        }}
        if proof and request.protocolVersion and \
                request.protocolVersion >= PlenumProtocolVersion.STATE_PROOF_SUPPORT.value:
            result[STATE_PROOF] = proof

        # Do not inline please, it makes debugging easier
        return result
