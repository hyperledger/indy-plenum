from abc import ABCMeta

from common.serializers.serialization import config_state_serializer
from plenum.common.constants import TXN_AUTHOR_AGREEMENT_VERSION, \
    TXN_AUTHOR_AGREEMENT_TEXT, TXN_AUTHOR_AGREEMENT_DIGEST, TXN_AUTHOR_AGREEMENT_RETIREMENT_TS, \
    TXN_AUTHOR_AGREEMENT_RATIFICATION_TS
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler
from plenum.server.request_handlers.static_taa_helper import StaticTAAHelper
from plenum.server.request_handlers.utils import encode_state_value


class BaseTAAHandler(WriteRequestHandler, metaclass=ABCMeta):

    def authorize(self, request):
        StaticTAAHelper.authorize(self.database_manager, request)

    def _set_taa_to_state(self, digest, seq_no, txn_time, text, version, ratification_ts, retirement_ts=None):

        state_value = {
            TXN_AUTHOR_AGREEMENT_TEXT: text,
            TXN_AUTHOR_AGREEMENT_VERSION: version,
            TXN_AUTHOR_AGREEMENT_RATIFICATION_TS: ratification_ts,
            TXN_AUTHOR_AGREEMENT_DIGEST: digest
        }
        if retirement_ts:
            state_value[TXN_AUTHOR_AGREEMENT_RETIREMENT_TS] = retirement_ts

        data = encode_state_value(state_value, seq_no, txn_time,
                                  serializer=config_state_serializer)

        self.state.set(StaticTAAHelper.state_path_taa_digest(digest), data)
