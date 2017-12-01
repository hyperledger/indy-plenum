from plenum.test.plugin.demo_plugin.transactions import DemoTransactions
from plenum.test.plugin.demo_plugin.constants import AUCTION_LEDGER_ID

LEDGER_IDS = {AUCTION_LEDGER_ID, }
AcceptableWriteTypes = {DemoTransactions.AUCTION_START.value,
                        DemoTransactions.AUCTION_END.value,
                        DemoTransactions.PLACE_BID.value}

AcceptableQueryTypes = {DemoTransactions.GET_BAL.value, }
