from common.serializers.serialization import state_roots_serializer
from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.genesis_txn.genesis_txn_initiator_from_file import GenesisTxnInitiatorFromFile
from plenum.bls.bls_bft_factory import create_default_bls_bft_factory
from plenum.common.ledger import Ledger
from plenum.common.ledger_manager import LedgerManager
from plenum.persistence.storage import initStorage
from plenum.server.batch_handlers.audit_batch_handler import AuditBatchHandler
from plenum.server.batch_handlers.config_batch_handler import ConfigBatchHandler
from plenum.server.batch_handlers.domain_batch_handler import DomainBatchHandler
from plenum.server.batch_handlers.pool_batch_handler import PoolBatchHandler
from plenum.server.batch_handlers.ts_store_batch_handler import TsStoreBatchHandler
from plenum.server.future_primaries_batch_handler import FuturePrimariesBatchHandler
from plenum.server.request_handlers.audit_handler import AuditTxnHandler
from plenum.server.request_handlers.get_txn_author_agreement_aml_handler import GetTxnAuthorAgreementAmlHandler
from plenum.server.request_handlers.get_txn_author_agreement_handler import GetTxnAuthorAgreementHandler
from plenum.server.request_handlers.get_txn_handler import GetTxnHandler
from plenum.server.request_handlers.node_handler import NodeHandler
from plenum.server.request_handlers.nym_handler import NymHandler

from plenum.common.constants import POOL_LEDGER_ID, AUDIT_LEDGER_ID, DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID, \
    NODE_PRIMARY_STORAGE_SUFFIX, BLS_PREFIX, BLS_LABEL, TS_LABEL, SEQ_NO_DB_LABEL, NODE_STATUS_DB_LABEL
from plenum.server.pool_manager import TxnPoolManager
from plenum.server.request_handlers.txn_author_agreement_aml_handler import TxnAuthorAgreementAmlHandler
from plenum.server.request_handlers.txn_author_agreement_handler import TxnAuthorAgreementHandler
from state.pruning_state import PruningState
from state.state import State
from storage.helper import initKeyValueStorage
from stp_core.common.log import getlogger


logger = getlogger()


class NodeBootstrap:

    def __init__(self, node):
        self.node = node

    def init_node(self, storage):
        self.init_storages(storage=storage)
        self.init_bls_bft()
        self.init_common_managers()
        self._init_write_request_validator()
        self.register_req_handlers()
        self.register_batch_handlers()
        self.register_common_handlers()
        self.upload_states()

    def init_state_ts_db_storage(self):
        ts_storage = self.node._get_state_ts_db_storage()
        self.node.db_manager.register_new_store(TS_LABEL, ts_storage)

    def init_seq_no_db_storage(self):
        seq_no_db_storage = self.node.loadSeqNoDB()
        self.node.db_manager.register_new_store(SEQ_NO_DB_LABEL, seq_no_db_storage)

    def init_node_status_db_storage(self):
        node_status_db = self.node.loadNodeStatusDB()
        self.node.db_manager.register_new_store(NODE_STATUS_DB_LABEL, node_status_db)

    def init_storages(self, storage=None):
        # Config ledger and state init
        self.node.db_manager.register_new_database(CONFIG_LEDGER_ID,
                                                   self.init_config_ledger(),
                                                   self.init_config_state(),
                                                   taa_acceptance_required=False)

        # Pool ledger init
        self.node.db_manager.register_new_database(POOL_LEDGER_ID,
                                                   self.init_pool_ledger(),
                                                   self.init_pool_state(),
                                                   taa_acceptance_required=False)

        # Domain ledger init
        self.node.db_manager.register_new_database(DOMAIN_LEDGER_ID,
                                                   storage or self.init_domain_ledger(),
                                                   self.init_domain_state(),
                                                   taa_acceptance_required=True)

        # Audit ledger init
        self.node.db_manager.register_new_database(AUDIT_LEDGER_ID,
                                                   self.init_audit_ledger(),
                                                   taa_acceptance_required=False)
        # StateTsDbStorage
        self.init_state_ts_db_storage()

        # seqNoDB storage
        self.init_seq_no_db_storage()

        # nodeStatusDB
        self.init_node_status_db_storage()

    def init_bls_bft(self):
        self.node.bls_bft = self._create_bls_bft()
        self.node.db_manager.register_new_store(BLS_LABEL, self.node.bls_bft.bls_store)

    def init_common_managers(self):
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

    def register_req_handlers(self):
        self.register_pool_req_handlers()
        self.register_domain_req_handlers()
        self.register_config_req_handlers()
        self.register_audit_req_handlers()
        self.register_action_req_handlers()

    def register_audit_req_handlers(self):
        audit_handler = AuditTxnHandler(database_manager=self.node.db_manager)
        self.node.write_manager.register_req_handler(audit_handler)

    def register_domain_req_handlers(self):
        nym_handler = NymHandler(self.node.config, self.node.db_manager)
        self.node.write_manager.register_req_handler(nym_handler)

    def register_pool_req_handlers(self):
        node_handler = NodeHandler(self.node.db_manager, self.node.bls_bft.bls_crypto_verifier)
        self.node.write_manager.register_req_handler(node_handler)

    def register_config_req_handlers(self):
        taa_aml_handler = TxnAuthorAgreementAmlHandler(database_manager=self.node.db_manager)
        taa_handler = TxnAuthorAgreementHandler(database_manager=self.node.db_manager)
        get_taa_aml_handler = GetTxnAuthorAgreementAmlHandler(database_manager=self.node.db_manager)
        get_taa_handler = GetTxnAuthorAgreementHandler(database_manager=self.node.db_manager)

        self.node.write_manager.register_req_handler(taa_aml_handler)
        self.node.write_manager.register_req_handler(taa_handler)

        self.node.read_manager.register_req_handler(get_taa_aml_handler)
        self.node.read_manager.register_req_handler(get_taa_handler)

    def register_action_req_handlers(self):
        pass

    def register_pool_batch_handlers(self):
        pool_b_h = PoolBatchHandler(self.node.db_manager)
        future_primaries_handler = FuturePrimariesBatchHandler(self.node.db_manager, self.node)
        self.node.write_manager.register_batch_handler(pool_b_h)
        self.node.write_manager.register_batch_handler(future_primaries_handler)

    def register_domain_batch_handlers(self):
        domain_b_h = DomainBatchHandler(self.node.db_manager)
        self.node.write_manager.register_batch_handler(domain_b_h)

    def register_config_batch_handlers(self):
        config_b_h = ConfigBatchHandler(self.node.db_manager)
        self.node.write_manager.register_batch_handler(config_b_h)

    def register_audit_batch_handlers(self):
        audit_b_h = AuditBatchHandler(self.node.db_manager)
        for lid in self.node.ledger_ids:
            self.node.write_manager.register_batch_handler(audit_b_h, ledger_id=lid)

    def register_ts_store_batch_handlers(self):
        ts_store_b_h = TsStoreBatchHandler(self.node.db_manager)
        for lid in [DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID]:
            self.node.write_manager.register_batch_handler(ts_store_b_h, ledger_id=lid)

    def register_common_handlers(self):
        get_txn_handler = GetTxnHandler(self, self.node.db_manager)
        for lid in self.node.ledger_ids:
            self.node.read_manager.register_req_handler(get_txn_handler, ledger_id=lid)
        self.register_ts_store_batch_handlers()

    def register_batch_handlers(self):
        self.register_pool_batch_handlers()
        self.register_domain_batch_handlers()
        self.register_config_batch_handlers()
        # Audit batch handler should be initiated the last
        self.register_audit_batch_handlers()

    def _init_write_request_validator(self):
        pass

    def init_pool_ledger(self):
        genesis_txn_initiator = GenesisTxnInitiatorFromFile(
            self.node.genesis_dir, self.node.config.poolTransactionsFile)
        tree = CompactMerkleTree(hashStore=self.node.getHashStore('pool'))
        return Ledger(tree,
                      dataDir=self.node.dataLocation,
                      fileName=self.node.config.poolTransactionsFile,
                      ensureDurability=self.node.config.EnsureLedgerDurability,
                      genesis_txn_initiator=genesis_txn_initiator)

    def init_domain_ledger(self):
        """
        This is usually an implementation of Ledger
        """
        if self.node.config.primaryStorage is None:
            # TODO: add a place for initialization of all ledgers, so it's
            # clear what ledgers we have and how they are initialized
            genesis_txn_initiator = GenesisTxnInitiatorFromFile(
                self.node.genesis_dir, self.node.config.domainTransactionsFile)
            tree = CompactMerkleTree(hashStore=self.node.getHashStore('domain'))
            return Ledger(tree,
                          dataDir=self.node.dataLocation,
                          fileName=self.node.config.domainTransactionsFile,
                          ensureDurability=self.node.config.EnsureLedgerDurability,
                          genesis_txn_initiator=genesis_txn_initiator)
        else:
            # TODO: we need to rethink this functionality
            return initStorage(self.node.config.primaryStorage,
                               name=self.node.name + NODE_PRIMARY_STORAGE_SUFFIX,
                               dataDir=self.node.dataLocation,
                               config=self.node.config)

    def init_config_ledger(self):
        return Ledger(CompactMerkleTree(hashStore=self.node.getHashStore('config')),
                      dataDir=self.node.dataLocation,
                      fileName=self.node.config.configTransactionsFile,
                      ensureDurability=self.node.config.EnsureLedgerDurability)

    def init_audit_ledger(self):
        return Ledger(CompactMerkleTree(hashStore=self.node.getHashStore('audit')),
                      dataDir=self.node.dataLocation,
                      fileName=self.node.config.auditTransactionsFile,
                      ensureDurability=self.node.config.EnsureLedgerDurability)

    # STATES
    def init_pool_state(self):
        return PruningState(
            initKeyValueStorage(
                self.node.config.poolStateStorage,
                self.node.dataLocation,
                self.node.config.poolStateDbName,
                db_config=self.node.config.db_state_config)
        )

    def init_domain_state(self):
        return PruningState(
            initKeyValueStorage(
                self.node.config.domainStateStorage,
                self.node.dataLocation,
                self.node.config.domainStateDbName,
                db_config=self.node.config.db_state_config)
        )

    def init_config_state(self):
        return PruningState(
            initKeyValueStorage(
                self.node.config.configStateStorage,
                self.node.dataLocation,
                self.node.config.configStateDbName,
                db_config=self.node.config.db_state_config)
        )

    # STATES INIT
    def init_state_from_ledger(self, state: State, ledger: Ledger):
        """
        If the trie is empty then initialize it by applying
        txns from ledger.
        """
        if state.isEmpty:
            logger.info('{} found state to be empty, recreating from '
                        'ledger'.format(self))
            for seq_no, txn in ledger.getAllTxn():
                txn = self.node.update_txn_with_extra_data(txn)
                self.node.write_manager.update_state(txn, isCommitted=True)
                state.commit(rootHash=state.headHash)

    def upload_pool_state(self):
        self.init_state_from_ledger(self.node.states[POOL_LEDGER_ID],
                                    self.node.poolLedger)
        logger.info(
            "{} initialized pool state: state root {}".format(
                self, state_roots_serializer.serialize(
                    bytes(self.node.states[POOL_LEDGER_ID].committedHeadHash))))

    def upload_domain_state(self):
        self.init_state_from_ledger(self.node.states[DOMAIN_LEDGER_ID],
                                    self.node.domainLedger)
        logger.info(
            "{} initialized domain state: state root {}".format(
                self, state_roots_serializer.serialize(
                    bytes(self.node.states[DOMAIN_LEDGER_ID].committedHeadHash))))

    def upload_config_state(self):
        self.init_state_from_ledger(self.node.states[CONFIG_LEDGER_ID],
                                    self.node.configLedger)
        logger.info(
            "{} initialized config state: state root {}".format(
                self, state_roots_serializer.serialize(
                    bytes(self.node.states[CONFIG_LEDGER_ID].committedHeadHash))))

    def upload_states(self):
        self.upload_pool_state()
        self.upload_config_state()
        self.upload_domain_state()

    def _create_bls_bft(self):
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
