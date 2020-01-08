from typing import Optional

from common.serializers.serialization import config_state_serializer
from plenum.common.constants import TXN_AUTHOR_AGREEMENT, CONFIG_LEDGER_ID, TXN_AUTHOR_AGREEMENT_VERSION, \
    TXN_AUTHOR_AGREEMENT_TEXT, TXN_AUTHOR_AGREEMENT_DIGEST, TXN_AUTHOR_AGREEMENT_RETIREMENT_TS, \
    TXN_AUTHOR_AGREEMENT_RATIFICATION_TS, TXN_AUTHOR_AGREEMENT_DISABLE
from plenum.common.exceptions import InvalidClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import get_payload_data, get_seq_no, get_txn_time
from plenum.common.util import SortedDict
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.base_taa_handler import BaseTAAHandler
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler
from plenum.server.request_handlers.static_taa_helper import StaticTAAHelper
from plenum.server.request_handlers.utils import encode_state_value, decode_state_value
import time

from state.trie.pruning_trie import rlp_decode


class TxnAuthorAgreementDisableHandler(BaseTAAHandler):

    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, TXN_AUTHOR_AGREEMENT_DISABLE, CONFIG_LEDGER_ID)

    def static_validation(self, request: Request):
        pass

    def dynamic_validation(self, request: Request, req_pp_time: Optional[int]):
        self._validate_request_type(request)
        self.authorize(request)
        if not self.state.get(StaticTAAHelper.state_path_taa_latest(), isCommitted=False):
            raise InvalidClientRequest(request.identifier, request.reqId,
                                       "Transaction author agreement is already disabled.")

    def update_state(self, txn, prev_result, request, is_committed=False):
        self._validate_txn_type(txn)
        seq_no = get_seq_no(txn)
        txn_time = get_txn_time(txn)
        _, taa_list = self.state.generate_state_proof_for_keys_with_prefix(StaticTAAHelper.state_path_taa_digest(""),
                                                                           serialize=False, get_value=True)
        for encode_key, encode_data in taa_list.items():
            taa = rlp_decode(encode_data)
            taa, last_seq_no, last_update_time = self._decode_state_value(taa[0])
            digest = StaticTAAHelper.get_digest_from_state_key(encode_key)
            if TXN_AUTHOR_AGREEMENT_RETIREMENT_TS not in taa or taa.get(TXN_AUTHOR_AGREEMENT_RETIREMENT_TS, 0) > txn_time:
                self._set_taa_to_state(digest, seq_no, txn_time, taa[TXN_AUTHOR_AGREEMENT_TEXT],
                                       taa[TXN_AUTHOR_AGREEMENT_VERSION],
                                       taa.get(TXN_AUTHOR_AGREEMENT_RATIFICATION_TS, last_update_time),
                                       retirement_ts=txn_time)
        self.state.remove(StaticTAAHelper.state_path_taa_latest())
