from _sha256 import sha256
from datetime import datetime, time
from typing import Dict, List, Optional, Tuple

from common.exceptions import LogicError
from common.serializers.serialization import pool_state_serializer, config_state_serializer
from plenum.common.config_util import getConfig

from plenum.common.constants import TXN_TYPE, POOL_LEDGER_ID, AML, TXN_AUTHOR_AGREEMENT_VERSION, \
    TXN_AUTHOR_AGREEMENT_TEXT, CONFIG_LEDGER_ID, AUDIT_LEDGER_ID, TXN_AUTHOR_AGREEMENT_RETIREMENT_TS, \
    TXN_AUTHOR_AGREEMENT_RATIFICATION_TS
from plenum.common.exceptions import InvalidClientTaaAcceptanceError, TaaAmlNotSetError
from plenum.server.batch_handlers.node_reg_handler import NodeRegHandler
from plenum.server.batch_handlers.primary_batch_handler import PrimaryBatchHandler

from plenum.server.request_handlers.utils import VALUE
from plenum.common.request import Request
from plenum.common.txn_util import get_type, get_txn_time, get_version
from plenum.common.types import f
from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler
from plenum.server.request_handlers.state_constants import MARKER_TAA, MARKER_TAA_AML
from plenum.server.request_handlers.utils import decode_state_value
from plenum.server.request_managers.request_manager import RequestManager
from stp_core.common.log import getlogger

logger = getlogger()


class WriteRequestManager(RequestManager):
    def __init__(self, database_manager: DatabaseManager):
        super().__init__()
        self.database_manager = database_manager
        self.batch_handlers = {}  # type: Dict[int,List[BatchRequestHandler]]
        self.state_serializer = pool_state_serializer
        self.audit_b_handler = None
        self.node_reg_handler = None
        self.primary_reg_handler = None
        self.config = getConfig()
        # TODO: combine dictionary request_handlers with _request_handlers_with_version.
        self._request_handlers_with_version = {}  # type: Dict[Tuple[int, str], List[WriteRequestHandler]]

    def is_valid_ledger_id(self, ledger_id):
        return ledger_id in self.ledger_ids

    def _add_handler(self, typ, handler):
        handler_list = self.request_handlers.setdefault(typ, [])
        handler_list.append(handler)

    def register_req_handler(self, handler: WriteRequestHandler, ledger_id=None, typ=None):
        if not isinstance(handler, WriteRequestHandler):
            raise LogicError
        self._register_req_handler(handler, ledger_id=ledger_id, typ=typ)

    def register_req_handler_with_version(self, handler: WriteRequestHandler, version, typ=None):
        if not isinstance(handler, WriteRequestHandler):
            raise LogicError
        if typ is None:
            typ = handler.txn_type
        self._request_handlers_with_version.setdefault((typ, version), [])
        self._request_handlers_with_version[(typ, version)].append(handler)

    def register_batch_handler(self, handler: BatchRequestHandler,
                               ledger_id=None, add_to_begin=False):
        # TODO: Probably it would be a good idea to improve this a bit
        #  - allow to register batch handler for ALL ledgers (including ones yet unknown)
        #    with just one function call
        #  - instead of add_to_begin flag allow handlers to order themselves by
        #    adding method order to BatchRequestHandler which takes other batch handler
        #    and returns one of BEFORE, AFTER, DONTCARE
        #  With these improvements both NodeBootstrap and plugins registration will
        #  become simpler and more robust
        if not isinstance(handler, BatchRequestHandler):
            raise LogicError
        ledger_id = ledger_id if ledger_id is not None else handler.ledger_id
        handler_list = self.batch_handlers.setdefault(ledger_id, [])
        if handler in handler_list:
            return
        if add_to_begin:
            handler_list.insert(0, handler)
        else:
            handler_list.append(handler)
        self.ledger_ids.add(ledger_id)
        if handler.ledger_id == AUDIT_LEDGER_ID:
            self.audit_b_handler = handler
        if isinstance(handler, NodeRegHandler):
            self.node_reg_handler = handler
        if isinstance(handler, PrimaryBatchHandler):
            self.primary_reg_handler = handler

    def remove_batch_handler(self, ledger_id):
        del self.batch_handlers[ledger_id]
        self.ledger_ids.remove(ledger_id)

    # WriteRequestHandler methods
    def static_validation(self, request: Request):
        handlers = self.request_handlers.get(request.operation[TXN_TYPE], None)
        if handlers is None:
            raise LogicError
        for handler in handlers:
            handler.static_validation(request)

    def dynamic_validation(self, request: Request, req_pp_time: Optional[int]):
        handlers = self.request_handlers.get(request.operation[TXN_TYPE], None)
        if handlers is None:
            raise LogicError
        for handler in handlers:
            handler.dynamic_validation(request, req_pp_time)

    def _get_handlers_by_version(self, txn):
        txn_type = get_type(txn)
        timestamp = get_txn_time(txn)
        version = self.database_manager.get_pool_version(timestamp)
        version = self.config.INDY_VERSION_MATCHING.get(version, version)
        if (txn_type, version) not in self._request_handlers_with_version:
            version = self.database_manager.get_txn_version(txn)

        handlers = self._request_handlers_with_version.get((txn_type, version)) \
            if (txn_type, version) in self._request_handlers_with_version \
            else self.request_handlers.get(txn_type, None)
        if handlers is None:
            raise LogicError
        return handlers

    def update_state(self, txn, request=None, isCommitted=False):
        handlers = self.request_handlers.get(get_type(txn), None)
        if handlers is None:
            raise LogicError
        updated_state = None
        for handler in handlers:
            updated_state = handler.update_state(txn, updated_state, request, isCommitted)

    def restore_state(self, txn, ledger_id):
        self.database_manager.update_state_version(txn)
        # TODO: add to TxnVersionController function `get_version(txn)`
        # to use a version from the txn and update it in the internal TxnVersionController version
        handlers = self._get_handlers_by_version(txn)
        updated_state = None
        for handler in handlers:
            updated_state = handler.update_state(txn, updated_state, None, is_committed=True)
        state = self.database_manager.get_state(ledger_id)
        if state:
            state.commit(rootHash=state.headHash)

    def apply_request(self, request, batch_ts):
        handlers = self.request_handlers.get(request.operation[TXN_TYPE], None)
        if handlers is None:
            raise LogicError
        start, txn, updated_state = handlers[0].apply_request(request, batch_ts, None)
        for handler in handlers[1:]:
            _, _, updated_state = handler.apply_request(request, batch_ts, updated_state)
        return start, txn

    def apply_forced_request(self, request):
        handlers = self.request_handlers.get(request.operation[TXN_TYPE], None)
        if handlers is None:
            raise LogicError
        for handler in handlers:
            handler.apply_forced_request(request)

    def revert_request(self):
        pass

    # BatchRequestHandler methods
    def post_apply_batch(self, three_pc_batch):
        handlers = self.batch_handlers.get(three_pc_batch.ledger_id, None)
        if handlers is None:
            raise LogicError
        prev_handler_result = handlers[0].post_batch_applied(three_pc_batch, None)
        for handler in handlers[1:]:
            prev_handler_result = handler.post_batch_applied(three_pc_batch, prev_handler_result)

    # TODO: no need to pass all these arguments explicitly here
    # we can use LedgerUncommittedTracker to get this values
    def commit_batch(self, three_pc_batch: ThreePcBatch):
        handlers = self.batch_handlers.get(three_pc_batch.ledger_id, None)
        if handlers is None:
            raise LogicError
        commited_txns = handlers[0].commit_batch(three_pc_batch, None)
        for handler in handlers[1:]:
            handler.commit_batch(three_pc_batch, commited_txns)
        return commited_txns

    def post_batch_rejected(self, ledger_id):
        handlers = self.batch_handlers.get(ledger_id, None)
        if handlers is None:
            raise LogicError
        prev_handler_result = handlers[0].post_batch_rejected(ledger_id, None)
        for handler in handlers[1:]:
            prev_handler_result = handler.post_batch_rejected(ledger_id, prev_handler_result)

    def transform_txn_for_ledger(self, txn):
        handlers = self.request_handlers.get(get_type(txn), None)
        if handlers is None:
            raise LogicError
        return handlers[0].transform_txn_for_ledger(txn)

    @property
    def pool_state(self):
        return self.database_manager.get_database(POOL_LEDGER_ID).state

    def get_node_data(self, nym, is_committed: bool = True):
        key = nym.encode()
        data = self.pool_state.get(key, is_committed)
        if not data:
            return {}
        return self.state_serializer.deserialize(data)

    def get_all_node_data_for_root_hash(self, root_hash):
        leaves = self.pool_state.get_all_leaves_for_root_hash(root_hash)
        raw_node_data = leaves.values()
        nodes = list(map(lambda x: self.state_serializer.deserialize(
            self.pool_state.get_decoded(x)), raw_node_data))
        return nodes

    @property
    def config_state(self):
        return self.database_manager.get_state(CONFIG_LEDGER_ID)

    def get_taa_digest(self, version: Optional[str] = None,
                       isCommitted: bool = True) -> Optional[str]:
        path = self._state_path_taa_latest() if version is None \
            else self._state_path_taa_version(version)
        res = self.config_state.get(path, isCommitted=isCommitted)
        if res is not None:
            return res.decode()

    # TODO return object as result instead
    def get_taa_data(self, digest: Optional[str] = None,
                     version: Optional[str] = None,
                     isCommitted: bool = True) -> Optional[Tuple[Dict, str]]:
        data = None
        if digest is None:
            digest = self.get_taa_digest(version=version, isCommitted=isCommitted)
        if digest is not None:
            data = self.config_state.get(
                self._state_path_taa_digest(digest),
                isCommitted=isCommitted
            )
        if data is not None:
            data = decode_state_value(
                data, serializer=config_state_serializer)
        return None if data is None else (data, digest)

    def get_taa_aml_data(self, version: Optional[str] = None, isCommitted: bool = True):
        path = self._state_path_taa_aml_latest() if version is None \
            else self._state_path_taa_aml_version(version)
        payload = self.config_state.get(path, isCommitted=isCommitted)
        if payload is None:
            return None
        return config_state_serializer.deserialize(payload)

    @staticmethod
    def _state_path_taa_latest() -> bytes:
        return "{marker}:latest". \
            format(marker=MARKER_TAA).encode()

    @staticmethod
    def _state_path_taa_version(version: str) -> bytes:
        return "{marker}:v:{version}". \
            format(marker=MARKER_TAA, version=version).encode()

    @staticmethod
    def _state_path_taa_digest(digest: str) -> bytes:
        return "{marker}:d:{digest}". \
            format(marker=MARKER_TAA, digest=digest).encode()

    @staticmethod
    def _taa_digest(text: str, version: str) -> str:
        return sha256('{}{}'.format(version, text).encode()).hexdigest()

    @staticmethod
    def _state_path_taa_aml_latest():
        return "{marker}:latest".format(marker=MARKER_TAA_AML).encode()

    @staticmethod
    def _state_path_taa_aml_version(version: str):
        return "{marker}:v:{version}".format(marker=MARKER_TAA_AML, version=version).encode()

    def on_catchup_finished(self):
        # ToDo: ugly thing, needs to be refactored
        self.audit_b_handler.on_catchup_finished()
        self.node_reg_handler.on_catchup_finished()

    def get_lid_for_request(self, request: Request):
        if request.operation.get(TXN_TYPE) is None:
            raise ValueError(
                "{} TXN_TYPE is not defined for request {}".format(self, request)
            )

        typ = request.operation[TXN_TYPE]
        return self.type_to_ledger_id[typ]

    def do_taa_validation(self, request: Request, req_pp_time: int, config):

        ledger_id = self.get_lid_for_request(request)
        if not self.database_manager.is_taa_acceptance_required(ledger_id):
            if request.taaAcceptance:
                raise InvalidClientTaaAcceptanceError(
                    request.identifier, request.reqId,
                    "Txn Author Agreement acceptance is not expected"
                    " and not allowed in requests for ledger id {}"
                    .format(ledger_id)
                )
            else:
                logger.trace(
                    "{} TAA acceptance passed for request {}: "
                    "not required for ledger id {}"
                    .format(self, request.reqId, ledger_id)
                )
                return

        if not self.get_taa_data(isCommitted=False):
            logger.trace(
                "{} TAA acceptance passed for request {}: "
                "taa is disabled".format(self, request.reqId)
            )
            return

        if not request.taaAcceptance:
            raise InvalidClientTaaAcceptanceError(
                request.identifier, request.reqId,
                "Txn Author Agreement acceptance is required for ledger with id {}"
                .format(ledger_id)
            )

        taa = None
        r_taa_a_digest = request.taaAcceptance.get(f.TAA_ACCEPTANCE_DIGEST.nm)
        taa_data = self.get_taa_data(digest=r_taa_a_digest, isCommitted=False)
        if taa_data is not None:
            (taa, taa_seq_no, taa_txn_time), taa_digest = taa_data
        else:
            raise InvalidClientTaaAcceptanceError(
                request.identifier, request.reqId,
                "Incorrect Txn Author Agreement(digest={}) in the request".format(r_taa_a_digest)
            )

        retired = taa.get(TXN_AUTHOR_AGREEMENT_RETIREMENT_TS)
        if retired and retired < req_pp_time:
            raise InvalidClientTaaAcceptanceError(
                request.identifier, request.reqId,
                "Txn Author Agreement is retired: version {}, seq_no {}, txn_time {}"
                .format(taa[TXN_AUTHOR_AGREEMENT_VERSION], taa_seq_no, taa_txn_time)
            )

        r_taa_a_ts = request.taaAcceptance[f.TAA_ACCEPTANCE_TIME.nm]
        try:
            datetime_r_taa = datetime.utcfromtimestamp(r_taa_a_ts)
        except ValueError:
            raise InvalidClientTaaAcceptanceError(
                request.identifier, request.reqId,
                "TAA_ACCEPTANCE_TIME = {} is out of range".format(r_taa_a_ts))
        if datetime_r_taa.time() != time(0):
            raise InvalidClientTaaAcceptanceError(
                request.identifier, request.reqId,
                "Txn Author Agreement acceptance time {}"
                " is too precise and is a privacy risk."
                .format(r_taa_a_ts))
        taa_txn_creation_time = taa.get(TXN_AUTHOR_AGREEMENT_RATIFICATION_TS, taa_txn_time)
        date_lowest = datetime.utcfromtimestamp(
            taa_txn_creation_time -
            config.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_BEFORE_TAA_TIME
        ).date()
        date_higest = datetime.utcfromtimestamp(
            req_pp_time +
            config.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_AFTER_PP_TIME
        ).date()
        if (datetime_r_taa.date() < date_lowest) or (datetime_r_taa.date() > date_higest):
            raise InvalidClientTaaAcceptanceError(
                request.identifier, request.reqId,
                "Txn Author Agreement acceptance time is inappropriate:"
                " provided {}, expected in [{}, {}]".format(r_taa_a_ts, date_lowest, date_higest)
            )

        taa_aml_data = self.get_taa_aml_data()
        if taa_aml_data is None:
            raise TaaAmlNotSetError(
                "Txn Author Agreement acceptance mechanism list is not defined"
            )

        taa_aml = taa_aml_data[VALUE][AML]
        r_taa_a_mech = request.taaAcceptance[f.TAA_ACCEPTANCE_MECHANISM.nm]
        if r_taa_a_mech not in taa_aml:
            raise InvalidClientTaaAcceptanceError(
                request.identifier, request.reqId,
                "Txn Author Agreement acceptance mechanism is inappropriate:"
                " provided {}, expected one of {}".format(r_taa_a_mech, sorted(taa_aml))
            )

        logger.trace(
            "{} TAA acceptance passed for request {}".format(self, request.reqId)
        )
