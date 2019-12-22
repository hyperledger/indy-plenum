from abc import ABCMeta

from common.serializers.serialization import config_state_serializer
from plenum.common.constants import TXN_AUTHOR_AGREEMENT, CONFIG_LEDGER_ID, TXN_AUTHOR_AGREEMENT_VERSION, \
    TXN_AUTHOR_AGREEMENT_TEXT, TXN_AUTHOR_AGREEMENT_DIGEST, TXN_AUTHOR_AGREEMENT_RETIREMENT_TS, \
    TXN_AUTHOR_AGREEMENT_RATIFICATION_TS
from plenum.common.exceptions import InvalidClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import get_payload_data, get_seq_no, get_txn_time
from plenum.common.util import SortedDict
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler
from plenum.server.request_handlers.static_taa_helper import StaticTAAHelper
from plenum.server.request_handlers.utils import encode_state_value, decode_state_value


class BaseTAAHandler(WriteRequestHandler, metaclass=ABCMeta):

    def _update_txn_author_agreement(self, digest, seq_no, txn_time, text=None, version=None, retired=None):
        taa_time = None
        ledger_data = self.get_from_state(StaticTAAHelper.state_path_taa_digest(digest))
        if ledger_data and ledger_data[0]:
            ledger_taa, last_seq_no, last_update_time = ledger_data
            taa_time = ledger_taa.get(TXN_AUTHOR_AGREEMENT_RATIFICATION_TS, last_update_time)
            text = ledger_taa.get(TXN_AUTHOR_AGREEMENT_TEXT)
            version = ledger_taa.get(TXN_AUTHOR_AGREEMENT_VERSION)

        state_value = {
            TXN_AUTHOR_AGREEMENT_TEXT: text,
            TXN_AUTHOR_AGREEMENT_VERSION: version,
            TXN_AUTHOR_AGREEMENT_DIGEST: digest
        }
        if retired:
            state_value[TXN_AUTHOR_AGREEMENT_RETIREMENT_TS] = retired
        state_value[TXN_AUTHOR_AGREEMENT_RATIFICATION_TS] = txn_time if taa_time is None else taa_time

        data = encode_state_value(state_value, seq_no, txn_time,
                                  serializer=config_state_serializer)

        self.state.set(StaticTAAHelper.state_path_taa_digest(digest), data)
        self.state.set(StaticTAAHelper.state_path_taa_version(version), digest)
        if not retired:
            self.state.set(StaticTAAHelper.state_path_taa_latest(), digest)

        # self.state.set(StaticTAAHelper.state_path_taa_digest(digest), data)
        # self.state.set(StaticTAAHelper.state_path_taa_latest(), digest)
        # self.state.set(StaticTAAHelper.state_path_taa_version(version), digest)

    def _set_taa_to_state(self, digest, seq_no, txn_time, text, version,
                          retirement_ts=None, ratification_ts=None):
        state_value = {
            TXN_AUTHOR_AGREEMENT_TEXT: text,
            TXN_AUTHOR_AGREEMENT_VERSION: version,
            TXN_AUTHOR_AGREEMENT_DIGEST: digest,
            TXN_AUTHOR_AGREEMENT_RATIFICATION_TS: txn_time if ratification_ts is None else ratification_ts
        }
        if retirement_ts:
            state_value[TXN_AUTHOR_AGREEMENT_RETIREMENT_TS] = retirement_ts

        data = encode_state_value(state_value, seq_no, txn_time,
                                  serializer=config_state_serializer)

        self.state.set(StaticTAAHelper.state_path_taa_digest(digest), data)

    def _add_taa_to_state(self, digest, seq_no, txn_time, text, version,
                          retirement_ts=None, ratification_ts=None):
        self._set_taa_to_state(digest, seq_no, txn_time, text, version,
                               retirement_ts, ratification_ts)
        self.state.set(StaticTAAHelper.state_path_taa_version(version), digest)
        self.state.set(StaticTAAHelper.state_path_taa_latest(), digest)


    def _update_taa_to_state(self, digest, seq_no, txn_time, text, version,
                          retirement_ts=None, ratification_ts=None):
        ledger_data = self.get_from_state(StaticTAAHelper.state_path_taa_digest(digest))
        if ledger_data and ledger_data[0]:
            ledger_taa, last_seq_no, last_update_time = ledger_data
            taa_time = ledger_taa.get(TXN_AUTHOR_AGREEMENT_RATIFICATION_TS, last_update_time)
            text = ledger_taa.get(TXN_AUTHOR_AGREEMENT_TEXT)
            version = ledger_taa.get(TXN_AUTHOR_AGREEMENT_VERSION)

        self._set_taa_to_state(digest, seq_no, txn_time, text, version,
                               retirement_ts, ratification_ts)
        self.state.set(StaticTAAHelper.state_path_taa_version(version), digest)
        self.state.set(StaticTAAHelper.state_path_taa_latest(), digest)

    def authorize(self, request):
        StaticTAAHelper.authorize(self.database_manager, request)
