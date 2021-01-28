from plenum.common.constants import CONFIG_LEDGER_ID
from plenum.server.request_handlers.utils import decode_state_value

from plenum.server.request_handlers.state_constants import MARKER_FROZEN_LEDGERS


class StaticLedgersFreezeHelper:

    LEDGER = "ledger"
    STATE = "state"
    SEQ_NO = "seq_no"

    @staticmethod
    def make_state_path_for_frozen_ledgers() -> bytes:
        return "{MARKER}:FROZEN_LEDGERS" \
            .format(MARKER=MARKER_FROZEN_LEDGERS).encode()

    @staticmethod
    def get_frozen_ledgers(config_state):
        encoded = config_state.get(StaticLedgersFreezeHelper.make_state_path_for_frozen_ledgers(), isCommitted=True)
        if not encoded:
            return {}
        frozen_ledgers, _, _ = decode_state_value(encoded)
        return frozen_ledgers

    @staticmethod
    def create_frozen_ledger_info(ledger_root, state_root, seq_no):
        return {StaticLedgersFreezeHelper.LEDGER: ledger_root,
                StaticLedgersFreezeHelper.STATE: state_root,
                StaticLedgersFreezeHelper.SEQ_NO: seq_no}

    @staticmethod
    def get_ledger_root(ledger_info) -> str:
        return ledger_info[StaticLedgersFreezeHelper.LEDGER]

    @staticmethod
    def get_state_root(ledger_info) -> str:
        return ledger_info[StaticLedgersFreezeHelper.STATE]

    @staticmethod
    def get_seq_no(ledger_info) -> str:
        return ledger_info[StaticLedgersFreezeHelper.SEQ_NO]
