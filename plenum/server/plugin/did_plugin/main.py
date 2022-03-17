from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.server.client_authn import CoreAuthNr
from plenum.server.plugin.did_plugin import DID_PLUGIN_LEDGER_ID
from plenum.server.plugin.did_plugin.batch_handlers.did_plugin_batch_handler import DIDBatchHandler
from plenum.server.plugin.did_plugin.config import get_config
from plenum.server.plugin.did_plugin.request_handlers.create_did_handler import CreateDIDHandler
from plenum.server.plugin.did_plugin.request_handlers.fetch_did import FetchDIDHandler
from plenum.server.plugin.did_plugin.storage import get_did_plugin_hash_store, \
    get_did_plugin_ledger, get_did_plugin_state


def integrate_plugin_in_node(node):
    node.config = get_config(node.config)
    hash_store = get_did_plugin_hash_store(node.dataLocation)
    ledger = get_did_plugin_ledger(node.dataLocation,
                                node.config.didPluginTransactionsFile,
                                hash_store, node.config)
    state = get_did_plugin_state(node.dataLocation,
                              node.config.didPluginStateDbName,
                              node.config)
    if DID_PLUGIN_LEDGER_ID not in node.ledger_ids:
        node.ledger_ids.append(DID_PLUGIN_LEDGER_ID)
    node.ledgerManager.addLedger(DID_PLUGIN_LEDGER_ID,
                                 ledger,
                                 postTxnAddedToLedgerClbk=node.postTxnFromCatchupAddedToLedger)
    node.on_new_ledger_added(DID_PLUGIN_LEDGER_ID)
    node.register_state(DID_PLUGIN_LEDGER_ID, state)

    did_dict = {}
    node.write_manager.register_req_handler(CreateDIDHandler(node.db_manager, did_dict))
    # node.write_manager.register_req_handler(AuctionEndHandler(node.db_manager, did_dict))
    # node.write_manager.register_req_handler(PlaceBidHandler(node.db_manager, did_dict))
    # node.read_manager.register_req_handler(GetBalHandler(node.db_manager))
    node.read_manager.register_req_handler(FetchDIDHandler(node.db_manager))
    # FIXME: find a generic way of registering DBs
    node.db_manager.register_new_database(lid=DID_PLUGIN_LEDGER_ID,
                                          ledger=ledger,
                                          state=state)
    node.write_manager.register_batch_handler(DIDBatchHandler(node.db_manager),
                                              ledger_id=DID_PLUGIN_LEDGER_ID,
                                              add_to_begin=True)
    node.write_manager.register_batch_handler(node.write_manager.node_reg_handler,
                                              ledger_id=DID_PLUGIN_LEDGER_ID)
    node.write_manager.register_batch_handler(node.write_manager.primary_reg_handler,
                                              ledger_id=DID_PLUGIN_LEDGER_ID)
    node.write_manager.register_batch_handler(node.write_manager.audit_b_handler,
                                              ledger_id=DID_PLUGIN_LEDGER_ID)

    did_plugin_authnr = CoreAuthNr(node.write_manager.txn_types,
                                node.read_manager.txn_types,
                                node.action_manager.txn_types,
                                node.states[DOMAIN_LEDGER_ID])
    node.clientAuthNr.register_authenticator(did_plugin_authnr)
    return node
