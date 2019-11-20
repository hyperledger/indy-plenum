from crypto.bls.bls_bft import BlsBft
from plenum.bls.bls_bft_factory import create_default_bls_bft_factory
from plenum.common.ledger_manager import LedgerManager
from plenum.server.batch_handlers.ts_store_batch_handler import TsStoreBatchHandler
from plenum.server.last_sent_pp_store_helper import LastSentPpStoreHelper
from plenum.server.ledgers_bootstrap import LedgersBootstrap
from plenum.server.request_handlers.get_txn_handler import GetTxnHandler

from plenum.common.constants import POOL_LEDGER_ID, DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID, \
    BLS_PREFIX, TS_LABEL, SEQ_NO_DB_LABEL, NODE_STATUS_DB_LABEL, LAST_SENT_PP_STORE_LABEL
from plenum.server.pool_manager import TxnPoolManager
from stp_core.common.log import getlogger

logger = getlogger()


class NodeBootstrap(LedgersBootstrap):

    def __init__(self, node):
        super().__init__(
            write_req_manager=node.write_manager,
            read_req_manager=node.read_manager,
            action_req_manager=node.action_manager,
            name=node.name,
            config=node.config,
            ledger_ids=node.ledger_ids)
        self.set_data_location(node.dataLocation)
        self.set_genesis_location(node.genesis_dir)
        self.node = node

    def init_state_ts_db_storage(self):
        ts_storage = self.node._get_state_ts_db_storage()
        self.node.db_manager.register_new_store(TS_LABEL, ts_storage)

    def init_seq_no_db_storage(self):
        seq_no_db_storage = self.node.loadSeqNoDB()
        self.node.db_manager.register_new_store(SEQ_NO_DB_LABEL, seq_no_db_storage)

    def init_node_status_db_storage(self):
        node_status_db = self.node.loadNodeStatusDB()
        self.node.db_manager.register_new_store(NODE_STATUS_DB_LABEL, node_status_db)

    def init_last_sent_pp_store(self):
        last_sent_pp_store = LastSentPpStoreHelper(self.node)
        self.node.db_manager.register_new_store(LAST_SENT_PP_STORE_LABEL, last_sent_pp_store)

    def _init_storages(self, domain_storage):
        super()._init_storages(domain_storage)

        # StateTsDbStorage
        self.init_state_ts_db_storage()

        # seqNoDB storage
        self.init_seq_no_db_storage()

        # nodeStatusDB
        self.init_node_status_db_storage()

        # last_sent_pp_store
        self.init_last_sent_pp_store()

    def _init_bls_bft(self):
        super()._init_bls_bft()
        self.node.bls_bft = self.bls_bft

    def _create_bls_bft(self) -> BlsBft:
        bls_factory = create_default_bls_bft_factory(self.node)
        bls_bft = bls_factory.create_bls_bft()
        if bls_bft.can_sign_bls():
            logger.display("{}BLS Signatures will be used for Node {}".format(BLS_PREFIX, self.node.name))
        else:
            # TODO: for now we allow that BLS is optional, so that we don't require it
            logger.warning(
                '{}Transactions will not be BLS signed by this Node, since BLS keys were not found. '
                'Please make sure that a script to init BLS keys was called (init_bls_keys),'
                ' and NODE txn was sent with BLS public keys.'.format(BLS_PREFIX))
        return bls_bft

    def _update_txn_with_extra_data(self, txn):
        return self.node.update_txn_with_extra_data(txn)

    def _init_common_managers(self):
        # Pool manager init
        self.node.poolManager = TxnPoolManager(self.node,
                                               self.node.poolLedger,
                                               self.node.states[POOL_LEDGER_ID],
                                               self.node.write_manager,
                                               self.node.ha,
                                               self.node.cliname,
                                               self.node.cliha)

        # Ledger manager init
        ledger_sync_order = self.node.ledger_ids
        self.node.ledgerManager = LedgerManager(self.node,
                                                postAllLedgersCaughtUp=self.node.allLedgersCaughtUp,
                                                preCatchupClbk=self.node.preLedgerCatchUp,
                                                postCatchupClbk=self.node.postLedgerCatchUp,
                                                ledger_sync_order=ledger_sync_order,
                                                metrics=self.node.metrics)

    def register_ts_store_batch_handlers(self):
        ts_store_b_h = TsStoreBatchHandler(self.node.db_manager)
        for lid in [DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID]:
            self.node.write_manager.register_batch_handler(ts_store_b_h, ledger_id=lid)

    def _register_common_handlers(self):
        get_txn_handler = GetTxnHandler(self.node, self.node.db_manager)
        for lid in self.node.ledger_ids:
            self.node.read_manager.register_req_handler(get_txn_handler, ledger_id=lid)
        self.register_ts_store_batch_handlers()
