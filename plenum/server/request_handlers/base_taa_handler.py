from abc import ABCMeta

from common.serializers.serialization import config_state_serializer
from plenum.common.constants import TXN_AUTHOR_AGREEMENT, CONFIG_LEDGER_ID, TXN_AUTHOR_AGREEMENT_VERSION, \
    TXN_AUTHOR_AGREEMENT_TEXT, TXN_AUTHOR_AGREEMENT_DIGEST, TXN_AUTHOR_AGREEMENT_RETIRED, \
    TXN_AUTHOR_AGREEMENT_TIMESTAMP
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
        ledger_taa = self.get_from_state(StaticTAAHelper.state_path_taa_digest(digest))[0]
        if ledger_taa:
            taa_time = ledger_taa.get(TXN_AUTHOR_AGREEMENT_TIMESTAMP)
            text = ledger_taa.get(TXN_AUTHOR_AGREEMENT_TEXT)
            version = ledger_taa.get(TXN_AUTHOR_AGREEMENT_VERSION)

        state_value = {
            TXN_AUTHOR_AGREEMENT_TEXT: text,
            TXN_AUTHOR_AGREEMENT_VERSION: version,
            TXN_AUTHOR_AGREEMENT_DIGEST: digest
        }
        if retired:
            state_value[TXN_AUTHOR_AGREEMENT_RETIRED] = retired
        state_value[TXN_AUTHOR_AGREEMENT_TIMESTAMP] = txn_time if taa_time is None else taa_time

        data = encode_state_value(state_value, seq_no, txn_time,
                                  serializer=config_state_serializer)

        self.state.set(StaticTAAHelper.state_path_taa_digest(digest), data)
        self.state.set(StaticTAAHelper.state_path_taa_version(version), digest)
        if not retired:
            self.state.set(StaticTAAHelper.state_path_taa_latest(), digest)

        # self.state.set(StaticTAAHelper.state_path_taa_digest(digest), data)
        # self.state.set(StaticTAAHelper.state_path_taa_latest(), digest)
        # self.state.set(StaticTAAHelper.state_path_taa_version(version), digest)

    def authorize(self, request):
        StaticTAAHelper.authorize(self.database_manager, request)
