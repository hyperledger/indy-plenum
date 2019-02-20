from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.plugin.demo_plugin import AUCTION_LEDGER_ID
from plenum.test.plugin.demo_plugin.auction_req_handler import AuctionReqHandler
from plenum.test.plugin.demo_plugin.client_authnr import AuctionAuthNr
from plenum.test.plugin.demo_plugin.config import get_config
from plenum.test.plugin.demo_plugin.storage import get_auction_hash_store, \
    get_auction_ledger, get_auction_state


def integrate_plugin_in_node(node):
    node.config = get_config(node.config)
    hash_store = get_auction_hash_store(node.dataLocation)
    ledger = get_auction_ledger(node.dataLocation,
                                node.config.auctionTransactionsFile,
                                hash_store, node.config)
    state = get_auction_state(node.dataLocation,
                              node.config.auctionStateDbName,
                              node.config)
    if AUCTION_LEDGER_ID not in node.ledger_ids:
        node.ledger_ids.append(AUCTION_LEDGER_ID)
    node.ledgerManager.addLedger(AUCTION_LEDGER_ID,
                                 ledger,
                                 postTxnAddedToLedgerClbk=node.postTxnFromCatchupAddedToLedger)
    node.on_new_ledger_added(AUCTION_LEDGER_ID)
    node.register_state(AUCTION_LEDGER_ID, state)
    auction_authnr = AuctionAuthNr(node.states[DOMAIN_LEDGER_ID])
    node.clientAuthNr.register_authenticator(auction_authnr)
    auction_req_handler = AuctionReqHandler(ledger, state)
    node.register_req_handler(auction_req_handler, AUCTION_LEDGER_ID)
    # FIXME: find a generic way of registering DBs
    node.db_manager.register_new_database(lid=AUCTION_LEDGER_ID,
                                          ledger=ledger,
                                          state=state)
    return node
