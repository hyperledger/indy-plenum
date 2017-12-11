from plenum.common.messages.fields import FixedLengthField
from plenum.test.plugin.demo_plugin.transactions import DemoTransactions
from plenum.test.plugin.demo_plugin.constants import AUCTION_LEDGER_ID


dummy_field_length = 10
LEDGER_IDS = {AUCTION_LEDGER_ID, }
CLIENT_REQUEST_FIELDS = {'fix_length_dummy':
                         FixedLengthField(dummy_field_length,
                         optional=True, nullable=True)}

AcceptableWriteTypes = {DemoTransactions.AUCTION_START.value,
                        DemoTransactions.AUCTION_END.value,
                        DemoTransactions.PLACE_BID.value}

AcceptableQueryTypes = {DemoTransactions.GET_BAL.value, }
