from _sha256 import sha256
from binascii import hexlify

from common.serializers.serialization import domain_state_serializer
from ledger.util import F
from plenum.common.constants import NYM, ROLE, STEWARD, DOMAIN_LEDGER_ID, \
    TXN_TIME, VERKEY, TARGET_NYM
from plenum.common.exceptions import UnauthorizedClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import get_payload_data, get_from, \
    get_seq_no, get_txn_time, get_request_data
from plenum.common.types import f
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler
from plenum.server.request_handlers.utils import is_steward, nym_to_state_key, get_nym_details
from stp_core.common.log import getlogger

logger = getlogger()


class NymHandler(WriteRequestHandler):
    state_serializer = domain_state_serializer

    def __init__(self, config, database_manager: DatabaseManager):
        super().__init__(database_manager, NYM, DOMAIN_LEDGER_ID)
        self.config = config
        self._steward_count = 0

    def static_validation(self, request: Request):
        pass

    def dynamic_validation(self, request: Request):
        self._validate_request_type(request)
        identifier, req_id, operation = get_request_data(request)
        error = None
        if not is_steward(self.state,
                          identifier, is_committed=False):
            error = "Only Steward is allowed to do these transactions"
        if operation.get(ROLE) == STEWARD:
            if self._steward_threshold_exceeded(self.config):
                error = "New stewards cannot be added by other stewards " \
                        "as there are already {} stewards in the system". \
                    format(self.config.stewardThreshold)
        if error:
            raise UnauthorizedClientRequest(identifier,
                                            req_id,
                                            error)

    def gen_state_key(self, txn):
        nym = get_payload_data(txn).get(TARGET_NYM)
        return self.make_state_path_for_nym(nym)

    def gen_txn_id(self, txn):
        return hexlify(self.gen_state_key(txn)).decode()

    def update_state(self, txn, prev_result, request, is_committed=False):
        self._validate_txn_type(txn)
        nym = get_payload_data(txn).get(TARGET_NYM)
        existing_data = get_nym_details(self.state, nym,
                                        is_committed=is_committed)
        txn_data = get_payload_data(txn)
        new_data = {}
        if not existing_data:
            # New nym being added to state, set the TrustAnchor
            new_data[f.IDENTIFIER.nm] = get_from(txn)
            # New nym being added to state, set the role and verkey to None, this makes
            # the state data always have a value for `role` and `verkey` since we allow
            # clients to omit specifying `role` and `verkey` in the request consider a
            # default value of None
            new_data[ROLE] = None
            new_data[VERKEY] = None

        if ROLE in txn_data:
            new_data[ROLE] = txn_data[ROLE]
        if VERKEY in txn_data:
            new_data[VERKEY] = txn_data[VERKEY]
        new_data[F.seqNo.name] = get_seq_no(txn)
        new_data[TXN_TIME] = get_txn_time(txn)
        self.__update_steward_count(new_data, existing_data)
        existing_data.update(new_data)
        val = self.state_serializer.serialize(existing_data)
        key = self.gen_state_key(txn)
        self.state.set(key, val)
        return existing_data

    def _steward_threshold_exceeded(self, config) -> bool:
        """We allow at most `stewardThreshold` number of  stewards to be added
        by other stewards"""
        return self._count_stewards() >= config.stewardThreshold

    def _count_stewards(self) -> int:
        """
        Count the number of stewards added to the pool transaction store
        Note: This is inefficient, a production use case of this function
        should require an efficient storage mechanism
        """
        return self._steward_count

    def __update_steward_count(self, new_data, existing_data=None):
        if not existing_data:
            existing_data = {}
            existing_data.setdefault(ROLE, "")
        if existing_data[ROLE] == STEWARD and new_data[ROLE] != STEWARD:
            self._steward_count -= 1
        elif existing_data[ROLE] != STEWARD and new_data[ROLE] == STEWARD:
            self._steward_count += 1

    @staticmethod
    def make_state_path_for_nym(did) -> bytes:
        return sha256(did.encode()).digest()
