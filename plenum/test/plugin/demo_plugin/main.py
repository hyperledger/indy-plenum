from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.server.client_authn import CoreAuthNr
from plenum.test.plugin.demo_plugin import AUCTION_LEDGER_ID
from plenum.test.plugin.demo_plugin.batch_handlers.auction_batch_handler import AuctionBatchHandler
from plenum.test.plugin.demo_plugin.config import get_config
from plenum.test.plugin.demo_plugin.request_handlers.auction_end_handler import AuctionEndHandler
from plenum.test.plugin.demo_plugin.request_handlers.auction_start_handler import AuctionStartHandler
from plenum.test.plugin.demo_plugin.request_handlers.get_bal_handler import GetBalHandler
from plenum.test.plugin.demo_plugin.request_handlers.place_bid_handler import PlaceBidHandler
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

    auctions = {}
    node.write_manager.register_req_handler(AuctionStartHandler(node.db_manager, auctions))
    node.write_manager.register_req_handler(AuctionEndHandler(node.db_manager, auctions))
    node.write_manager.register_req_handler(PlaceBidHandler(node.db_manager, auctions))
    node.read_manager.register_req_handler(GetBalHandler(node.db_manager))
    # FIXME: find a generic way of registering DBs
    node.db_manager.register_new_database(lid=AUCTION_LEDGER_ID,
                                          ledger=ledger,
                                          state=state)
    node.write_manager.register_batch_handler(AuctionBatchHandler(node.db_manager),
                                              ledger_id=AUCTION_LEDGER_ID,
                                              add_to_begin=True)
    node.write_manager.register_batch_handler(node.write_manager.node_reg_handler,
                                              ledger_id=AUCTION_LEDGER_ID)
    node.write_manager.register_batch_handler(node.write_manager.primary_reg_handler,
                                              ledger_id=AUCTION_LEDGER_ID)
    node.write_manager.register_batch_handler(node.write_manager.audit_b_handler,
                                              ledger_id=AUCTION_LEDGER_ID)

    auction_authnr = CoreAuthNr(node.write_manager.txn_types,
                                node.read_manager.txn_types,
                                node.action_manager.txn_types,
                                node.states[DOMAIN_LEDGER_ID])
    node.clientAuthNr.register_authenticator(auction_authnr)
    return node
