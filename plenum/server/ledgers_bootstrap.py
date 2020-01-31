from typing import Any, List, Optional

from common.exceptions import LogicError
from common.serializers.serialization import state_roots_serializer
from crypto.bls.bls_bft import BlsBft
from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.genesis_txn.genesis_txn_initiator import GenesisTxnInitiator
from ledger.genesis_txn.genesis_txn_initiator_from_file import GenesisTxnInitiatorFromFile
from ledger.genesis_txn.genesis_txn_initiator_from_mem import GenesisTxnInitiatorFromMem
from plenum.common.constants import AUDIT_LEDGER_ID, POOL_LEDGER_ID, CONFIG_LEDGER_ID, DOMAIN_LEDGER_ID, \
    NODE_PRIMARY_STORAGE_SUFFIX, BLS_LABEL, HS_MEMORY
from plenum.common.ledger import Ledger
from plenum.persistence.storage import initStorage
from plenum.server.batch_handlers.audit_batch_handler import AuditBatchHandler
from plenum.server.batch_handlers.config_batch_handler import ConfigBatchHandler
from plenum.server.batch_handlers.domain_batch_handler import DomainBatchHandler
from plenum.server.batch_handlers.node_reg_handler import NodeRegHandler
from plenum.server.batch_handlers.pool_batch_handler import PoolBatchHandler
from plenum.server.batch_handlers.primary_batch_handler import PrimaryBatchHandler
from plenum.server.request_handlers.audit_handler import AuditTxnHandler
from plenum.server.request_handlers.get_txn_author_agreement_aml_handler import GetTxnAuthorAgreementAmlHandler
from plenum.server.request_handlers.get_txn_author_agreement_handler import GetTxnAuthorAgreementHandler
from plenum.server.request_handlers.node_handler import NodeHandler
from plenum.server.request_handlers.nym_handler import NymHandler
from plenum.server.request_handlers.txn_author_agreement_aml_handler import TxnAuthorAgreementAmlHandler
from plenum.server.request_handlers.txn_author_agreement_disable_handler import TxnAuthorAgreementDisableHandler
from plenum.server.request_handlers.txn_author_agreement_handler import TxnAuthorAgreementHandler
from plenum.server.request_managers.action_request_manager import ActionRequestManager
from plenum.server.request_managers.read_request_manager import ReadRequestManager
from plenum.server.request_managers.write_request_manager import WriteRequestManager
from state.pruning_state import PruningState
from storage.helper import initHashStore, initKeyValueStorage
from storage.kv_in_memory import KeyValueStorageInMemory
from stp_core.common.log import getlogger

logger = getlogger()


class LedgersBootstrap:
    # TODO: Create ReqManagers from inside and make them properties just like bls_bft?

    def __init__(self,
                 write_req_manager: WriteRequestManager,
                 read_req_manager: ReadRequestManager,
                 action_req_manager: ActionRequestManager,
                 name: str,
                 config: Any,
                 ledger_ids: List[int]):
        self.write_manager = write_req_manager
        self.read_manager = read_req_manager
        self.action_manager = action_req_manager
        self.db_manager = write_req_manager.database_manager
        self._bls_bft = None  # type: Optional[BlsBft]
        # TODO: vvv Move into some node config container class? vvv
        self.name = name
        self.config = config
        self.ledger_ids = ledger_ids
        self.data_location = None
        self.pool_genesis = None  # type: Optional[GenesisTxnInitiator]
        self.domain_genesis = None  # type: Optional[GenesisTxnInitiator]
        # TODO: ^^^

    def set_data_location(self, data_location: str):
        self.data_location = data_location

    def set_genesis_location(self, genesis_dir: str):
        pool_genesis_file = getattr(self.config, "poolTransactionsFile")
        self.pool_genesis = GenesisTxnInitiatorFromFile(genesis_dir, pool_genesis_file)

        domain_genesis_file = getattr(self.config, "domainTransactionsFile")
        self.domain_genesis = GenesisTxnInitiatorFromFile(genesis_dir, domain_genesis_file)

    def set_genesis_transactions(self, pool_txns: List, domain_txns: List):
        self.pool_genesis = GenesisTxnInitiatorFromMem(pool_txns)
        self.domain_genesis = GenesisTxnInitiatorFromMem(domain_txns)

    def init(self, domain_storage=None):
        self._init_storages(domain_storage=domain_storage)
        self._init_bls_bft()
        self._init_common_managers()
        self._init_write_request_validator()
        self._register_req_handlers()
        self._register_batch_handlers()
        self._register_common_handlers()

    @property
    def bls_bft(self) -> BlsBft:
        if self._bls_bft is None:
            raise LogicError("Tried to access BlsBft before initialization")
        return self._bls_bft

    def _create_bls_bft(self) -> BlsBft:
        raise NotImplemented

    def _update_txn_with_extra_data(self, txn):
        raise NotImplemented

    def _init_storages(self, domain_storage):
        self.db_manager.register_new_database(CONFIG_LEDGER_ID,
                                              self._create_ledger('config'),
                                              self._create_state('config'),
                                              taa_acceptance_required=False)

        self.db_manager.register_new_database(POOL_LEDGER_ID,
                                              self._create_ledger('pool', self.pool_genesis),
                                              self._create_state('pool'),
                                              taa_acceptance_required=False)

        self.db_manager.register_new_database(DOMAIN_LEDGER_ID,
                                              domain_storage or self._create_domain_ledger(),
                                              self._create_state('domain'),
                                              taa_acceptance_required=True)

        self.db_manager.register_new_database(AUDIT_LEDGER_ID,
                                              self._create_ledger('audit'),
                                              taa_acceptance_required=False)

    def _init_bls_bft(self):
        self._bls_bft = self._create_bls_bft()
        self.db_manager.register_new_store(BLS_LABEL, self.bls_bft.bls_store)

    def _init_common_managers(self):
        pass

    def _init_write_request_validator(self):
        pass

    def _register_req_handlers(self):
        self._register_pool_req_handlers()
        self._register_domain_req_handlers()
        self._register_config_req_handlers()
        self._register_audit_req_handlers()
        self._register_action_req_handlers()

    def _register_pool_req_handlers(self):
        node_handler = NodeHandler(self.db_manager, self.bls_bft.bls_crypto_verifier)
        self.write_manager.register_req_handler(node_handler)

    def _register_domain_req_handlers(self):
        nym_handler = NymHandler(self.config, self.db_manager)
        self.write_manager.register_req_handler(nym_handler)

    def _register_config_req_handlers(self):
        taa_aml_handler = TxnAuthorAgreementAmlHandler(database_manager=self.db_manager)
        taa_handler = TxnAuthorAgreementHandler(database_manager=self.db_manager)
        taa_disable_handler = TxnAuthorAgreementDisableHandler(database_manager=self.db_manager)
        get_taa_aml_handler = GetTxnAuthorAgreementAmlHandler(database_manager=self.db_manager)
        get_taa_handler = GetTxnAuthorAgreementHandler(database_manager=self.db_manager)

        self.write_manager.register_req_handler(taa_aml_handler)
        self.write_manager.register_req_handler(taa_handler)
        self.write_manager.register_req_handler(taa_disable_handler)

        self.read_manager.register_req_handler(get_taa_aml_handler)
        self.read_manager.register_req_handler(get_taa_handler)

    def _register_audit_req_handlers(self):
        audit_handler = AuditTxnHandler(database_manager=self.db_manager)
        self.write_manager.register_req_handler(audit_handler)

    def _register_action_req_handlers(self):
        pass

    def _register_batch_handlers(self):
        self._register_pool_batch_handlers()
        self._register_domain_batch_handlers()
        self._register_config_batch_handlers()
        self._register_node_reg_handlers()
        # Audit batch handler should be initiated the last
        self._register_audit_batch_handlers()

    def _register_pool_batch_handlers(self):
        pool_b_h = PoolBatchHandler(self.db_manager)
        self.write_manager.register_batch_handler(pool_b_h)

    def _register_domain_batch_handlers(self):
        domain_b_h = DomainBatchHandler(self.db_manager)
        self.write_manager.register_batch_handler(domain_b_h)

    def _register_config_batch_handlers(self):
        config_b_h = ConfigBatchHandler(self.db_manager)
        self.write_manager.register_batch_handler(config_b_h)

    def _register_audit_batch_handlers(self):
        audit_b_h = AuditBatchHandler(self.db_manager)
        for lid in self.ledger_ids:
            self.write_manager.register_batch_handler(audit_b_h, ledger_id=lid)

    def _register_node_reg_handlers(self):
        node_reg_handler = NodeRegHandler(self.db_manager)
        primary_reg_handler = PrimaryBatchHandler(self.db_manager, node_reg_handler)
        self.write_manager.register_req_handler(node_reg_handler)
        for lid in self.ledger_ids:
            # register NodeRegHandler first
            self.write_manager.register_batch_handler(node_reg_handler, ledger_id=lid)
            self.write_manager.register_batch_handler(primary_reg_handler, ledger_id=lid)
        return node_reg_handler

    def _register_common_handlers(self):
        pass

    def upload_states(self, ledger_ids=[POOL_LEDGER_ID,
                                        CONFIG_LEDGER_ID,
                                        DOMAIN_LEDGER_ID]):
        for l_id in ledger_ids:
            self._init_state_from_ledger(l_id)

    def _create_ledger(self, name: str, genesis: Optional[GenesisTxnInitiator] = None) -> Ledger:
        hs_type = HS_MEMORY if self.data_location is None else None
        hash_store = initHashStore(self.data_location, name, self.config, hs_type=hs_type)
        txn_file_name = getattr(self.config, "{}TransactionsFile".format(name))

        txn_log_storage = None
        if self.data_location is None:
            txn_log_storage = KeyValueStorageInMemory()

        return Ledger(CompactMerkleTree(hashStore=hash_store),
                      dataDir=self.data_location,
                      fileName=txn_file_name,
                      transactionLogStore=txn_log_storage,
                      ensureDurability=self.config.EnsureLedgerDurability,
                      genesis_txn_initiator=genesis)

    def _create_domain_ledger(self) -> Ledger:
        if self.config.primaryStorage is None:
            # TODO: add a place for initialization of all ledgers, so it's
            # clear what ledgers we have and how they are initialized
            return self._create_ledger('domain', self.domain_genesis)
        else:
            # TODO: we need to rethink this functionality
            return initStorage(self.config.primaryStorage,
                               name=self.name + NODE_PRIMARY_STORAGE_SUFFIX,
                               dataDir=self.data_location,
                               config=self.config)

    def _create_state(self, name: str) -> PruningState:
        storage_name = getattr(self.config, "{}StateStorage".format(name))
        db_name = getattr(self.config, "{}StateDbName".format(name))
        if self.data_location is not None:
            return PruningState(
                initKeyValueStorage(
                    storage_name,
                    self.data_location,
                    db_name,
                    db_config=self.config.db_state_config))
        else:
            return PruningState(KeyValueStorageInMemory())

    def _init_state_from_ledger(self, ledger_id: int):
        """
        If the trie is empty then initialize it by applying
        txns from ledger.
        """
        state = self.db_manager.get_state(ledger_id)
        if not state or state.closed:
            return
        if state.isEmpty:
            logger.info('{} found state to be empty, recreating from ledger {}'.format(self, ledger_id))
            ledger = self.db_manager.get_ledger(ledger_id)
            for seq_no, txn in ledger.getAllTxn():
                txn = self._update_txn_with_extra_data(txn)
                self.write_manager.restore_state(txn, ledger_id)

        logger.info(
            "{} initialized state for ledger {}: state root {}".format(
                self, ledger_id,
                state_roots_serializer.serialize(bytes(state.committedHeadHash))))
