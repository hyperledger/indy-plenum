from common.serializers.serialization import state_roots_serializer
from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.genesis_txn.genesis_txn_initiator_from_file import GenesisTxnInitiatorFromFile
from plenum.bls.bls_bft_factory import create_default_bls_bft_factory
from plenum.common.ledger import Ledger
from plenum.persistence.storage import initStorage
from plenum.server.batch_handlers.audit_batch_handler import AuditBatchHandler
from plenum.server.batch_handlers.config_batch_handler import ConfigBatchHandler
from plenum.server.batch_handlers.domain_batch_handler import DomainBatchHandler
from plenum.server.batch_handlers.pool_batch_handler import PoolBatchHandler
from plenum.server.request_handlers.get_txn_handler import GetTxnHandler
from plenum.server.request_handlers.node_handler import NodeHandler
from plenum.server.request_handlers.nym_handler import NymHandler
from plenum.server.request_managers.action_request_manager import ActionRequestManager

from plenum.common.constants import POOL_LEDGER_ID, AUDIT_LEDGER_ID, DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID, \
    NODE_PRIMARY_STORAGE_SUFFIX, BLS_PREFIX, BLS_LABEL
from plenum.server.database_manager import DatabaseManager
from plenum.server.pool_manager import HasPoolManager
from plenum.server.request_managers.read_request_manager import ReadRequestManager
from plenum.server.request_managers.write_request_manager import WriteRequestManager
from state.pruning_state import PruningState
from state.state import State
from storage.helper import initKeyValueStorage
from stp_core.common.log import getlogger


logger = getlogger()


class NodeBootstrap:

    def init_node(self, storage):
        self.db_manager = DatabaseManager()
        self.init_req_managers()
        self.init_storages(storage=storage)
        self.init_bls_bft()
        self.init_common_managers()
        self._init_write_request_validator()
        self.register_req_handlers()
        self.register_common_handlers()
        self.register_batch_handlers()

    def init_req_managers(self):
        self.write_manager = WriteRequestManager(self.db_manager)
        self.read_manager = ReadRequestManager()
        self.action_manager = ActionRequestManager()

    def init_storages(self, storage=None):
        # Config ledger and state init
        self.db_manager.register_new_database(CONFIG_LEDGER_ID,
                                              self.init_config_ledger(),
                                              self.init_config_state())

        # Pool ledger init
        self.db_manager.register_new_database(POOL_LEDGER_ID,
                                              self.init_pool_ledger(),
                                              self.init_pool_state())

        # Domain ledger init
        self.db_manager.register_new_database(DOMAIN_LEDGER_ID,
                                              storage or self.init_domain_ledger(),
                                              self.init_domain_state())

        # Audit ledger init
        self.db_manager.register_new_database(AUDIT_LEDGER_ID,
                                              self.init_audit_ledger())

    def init_bls_bft(self):
        self.bls_bft = self._create_bls_bft()
        self.db_manager.register_new_store(BLS_LABEL, self.bls_bft.bls_store)

    def init_common_managers(self):
        # Pool manager init
        HasPoolManager.__init__(self, self.poolLedger,
                                self.states[POOL_LEDGER_ID],
                                self.write_manager,
                                self.ha,
                                self.cliname,
                                self.cliha)

    def register_req_handlers(self):
        self.register_pool_req_handlers()
        self.register_domain_req_handlers()
        self.register_config_req_handlers()
        self.register_audit_req_handlers()
        self.register_action_req_handlers()

    def register_audit_req_handlers(self):
        pass

    def register_domain_req_handlers(self):
        nym_handler = NymHandler(self.config, self.db_manager)
        self.write_manager.register_req_handler(nym_handler)

    def register_pool_req_handlers(self):
        node_handler = NodeHandler(self.db_manager, self.bls_bft.bls_crypto_verifier)
        self.write_manager.register_req_handler(node_handler)

    def register_config_req_handlers(self):
        pass

    def register_action_req_handlers(self):
        pass

    def register_pool_batch_handlers(self):
        pool_b_h = PoolBatchHandler(self.db_manager)
        self.write_manager.register_batch_handler(pool_b_h)

    def register_domain_batch_handlers(self):
        domain_b_h = DomainBatchHandler(self.db_manager)
        self.write_manager.register_batch_handler(domain_b_h)

    def register_config_batch_handlers(self):
        config_b_h = ConfigBatchHandler(self.db_manager)
        self.write_manager.register_batch_handler(config_b_h)

    def register_audit_batch_handlers(self):
        audit_b_h = AuditBatchHandler(self.db_manager)
        self.write_manager.register_batch_handler(audit_b_h)

    def register_common_handlers(self):
        get_txn_handler = GetTxnHandler(self, self.db_manager)
        self.read_manager.register_req_handler(get_txn_handler)

    def register_batch_handlers(self):
        self.register_pool_batch_handlers()
        self.register_domain_batch_handlers()
        self.register_config_batch_handlers()
        self.register_audit_batch_handlers()

    def _init_write_request_validator(self):
        pass

    def init_pool_ledger(self):
        genesis_txn_initiator = GenesisTxnInitiatorFromFile(
            self.genesis_dir, self.config.poolTransactionsFile)
        tree = CompactMerkleTree(hashStore=self.getHashStore('pool'))
        return Ledger(tree,
                      dataDir=self.dataLocation,
                      fileName=self.config.poolTransactionsFile,
                      ensureDurability=self.config.EnsureLedgerDurability,
                      genesis_txn_initiator=genesis_txn_initiator)

    def init_domain_ledger(self):
        """
        This is usually an implementation of Ledger
        """
        if self.config.primaryStorage is None:
            # TODO: add a place for initialization of all ledgers, so it's
            # clear what ledgers we have and how they are initialized
            genesis_txn_initiator = GenesisTxnInitiatorFromFile(
                self.genesis_dir, self.config.domainTransactionsFile)
            tree = CompactMerkleTree(hashStore=self.getHashStore('domain'))
            return Ledger(tree,
                          dataDir=self.dataLocation,
                          fileName=self.config.domainTransactionsFile,
                          ensureDurability=self.config.EnsureLedgerDurability,
                          genesis_txn_initiator=genesis_txn_initiator)
        else:
            # TODO: we need to rethink this functionality
            return initStorage(self.config.primaryStorage,
                               name=self.name + NODE_PRIMARY_STORAGE_SUFFIX,
                               dataDir=self.dataLocation,
                               config=self.config)

    def init_config_ledger(self):
        return Ledger(CompactMerkleTree(hashStore=self.getHashStore('config')),
                      dataDir=self.dataLocation,
                      fileName=self.config.configTransactionsFile,
                      ensureDurability=self.config.EnsureLedgerDurability)

    def init_audit_ledger(self):
        return Ledger(CompactMerkleTree(hashStore=self.getHashStore('audit')),
                      dataDir=self.dataLocation,
                      fileName=self.config.auditTransactionsFile,
                      ensureDurability=self.config.EnsureLedgerDurability)

    # STATES
    def init_pool_state(self):
        return PruningState(
            initKeyValueStorage(
                self.config.poolStateStorage,
                self.dataLocation,
                self.config.poolStateDbName,
                db_config=self.config.db_state_config)
        )

    def init_domain_state(self):
        return PruningState(
            initKeyValueStorage(
                self.config.domainStateStorage,
                self.dataLocation,
                self.config.domainStateDbName,
                db_config=self.config.db_state_config)
        )

    def init_config_state(self):
        return PruningState(
            initKeyValueStorage(
                self.config.configStateStorage,
                self.dataLocation,
                self.config.configStateDbName,
                db_config=self.config.db_state_config)
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
                txn = self.update_txn_with_extra_data(txn)
                self.write_manager.update_state(txn, isCommitted=True)
                state.commit(rootHash=state.headHash)

    def upload_pool_state(self):
        self.init_state_from_ledger(self.states[POOL_LEDGER_ID],
                                    self.poolLedger)
        logger.info(
            "{} initialized pool state: state root {}".format(
                self, state_roots_serializer.serialize(
                    bytes(self.states[POOL_LEDGER_ID].committedHeadHash))))

    def upload_domain_state(self):
        self.init_state_from_ledger(self.states[DOMAIN_LEDGER_ID],
                                    self.domainLedger)
        logger.info(
            "{} initialized domain state: state root {}".format(
                self, state_roots_serializer.serialize(
                    bytes(self.states[DOMAIN_LEDGER_ID].committedHeadHash))))

    def upload_config_state(self):
        self.init_state_from_ledger(self.states[CONFIG_LEDGER_ID],
                                    self.configLedger)
        logger.info(
            "{} initialized config state: state root {}".format(
                self, state_roots_serializer.serialize(
                    bytes(self.states[CONFIG_LEDGER_ID].committedHeadHash))))

    def upload_states(self):
        self.upload_pool_state()
        self.upload_config_state()
        self.upload_domain_state()

    def _create_bls_bft(self):
        bls_factory = create_default_bls_bft_factory(self)
        bls_bft = bls_factory.create_bls_bft()
        if bls_bft.can_sign_bls():
            logger.display("{}BLS Signatures will be used for Node {}".format(BLS_PREFIX, self.name))
        else:
            # TODO: for now we allow that BLS is optional, so that we don't require it
            logger.warning(
                '{}Transactions will not be BLS signed by this Node, since BLS keys were not found. '
                'Please make sure that a script to init BLS keys was called (init_bls_keys),'
                ' and NODE txn was sent with BLS public keys.'.format(BLS_PREFIX))
        return bls_bft
