# inter-node communication
from enum import IntEnum, unique

from plenum.common.plenum_protocol_version import PlenumProtocolVersion
from plenum.common.roles import Roles
from plenum.common.transactions import PlenumTransactions
from plenum.common.util import UniqueSet

NOMINATE = "NOMINATE"
REELECTION = "REELECTION"
PRIMARY = "PRIMARY"
PRIMDEC = "PRIMARYDECIDED"

BATCH = "BATCH"

REQACK = "REQACK"
REQNACK = "REQNACK"
REJECT = "REJECT"

POOL_LEDGER_TXNS = "POOL_LEDGER_TXNS"

PROPAGATE = "PROPAGATE"

PREPREPARE = "PREPREPARE"
PREPARE = "PREPARE"
COMMIT = "COMMIT"
CHECKPOINT = "CHECKPOINT"
CHECKPOINT_STATE = "CHECKPOINT_STATE"
THREE_PC_STATE = "THREE_PC_STATE"
UPDATE_BLS_MULTI_SIG = "UPDATE_BLS_MULTI_SIG"

REPLY = "REPLY"

ORDERED = "ORDERED"
REQKEY = "REQKEY"

INSTANCE_CHANGE = "INSTANCE_CHANGE"
BACKUP_INSTANCE_FAULTY = "BACKUP_INSTANCE_FAULTY"
VIEW_CHANGE_DONE = "VIEW_CHANGE_DONE"
CURRENT_STATE = "CURRENT_STATE"

LEDGER_STATUS = "LEDGER_STATUS"
CONSISTENCY_PROOF = "CONSISTENCY_PROOF"
CATCHUP_REQ = "CATCHUP_REQ"
CATCHUP_REP = "CATCHUP_REP"
MESSAGE_REQUEST = 'MESSAGE_REQUEST'
MESSAGE_RESPONSE = 'MESSAGE_RESPONSE'
OBSERVED_DATA = 'OBSERVED_DATA'
BATCH_COMMITTED = 'BATCH_COMMITTED'
VIEW_CHANGE_START = 'ViewChangeStart'
VIEW_CHANGE_CONTINUE = 'ViewChangeContinue'

BLACKLIST = "BLACKLIST"

THREE_PC_PREFIX = "3PC: "
MONITORING_PREFIX = "MONITORING: "
VIEW_CHANGE_PREFIX = "VIEW CHANGE: "
CATCH_UP_PREFIX = "CATCH-UP: "
PRIMARY_SELECTION_PREFIX = "PRIMARY SELECTION: "
BLS_PREFIX = "BLS: "
OBSERVER_PREFIX = "OBSERVER: "


PROPOSED_VIEW_NO = "proposed_view_no"
NAME = "name"
VERSION = "version"
IP = "ip"
PORT = "port"
KEYS = "keys"
TYPE = "type"
TXN_TYPE = "type"
TXN_ID = "txnId"
ORIGIN = "origin"
# Use f.IDENTIFIER.nm
IDENTIFIER = "identifier"
TARGET_NYM = "dest"
DATA = "data"
RAW = "raw"
ENC = "enc"
HASH = "hash"
ALIAS = "alias"
PUBKEY = "pubkey"
VERKEY = "verkey"
BLS_KEY = "blskey"
BLS_KEY_PROOF = "blskey_pop"
NYM_KEY = "NYM"
NODE_IP = "node_ip"
NODE_PORT = "node_port"
CLIENT_IP = "client_ip"
CLIENT_PORT = "client_port"
# CHANGE_HA = "CHANGE_HA"
# CHANGE_KEYS = "CHANGE_KEYS"
SERVICES = "services"
VALIDATOR = "VALIDATOR"
CLIENT = "CLIENT"
ROLE = 'role'
NONCE = 'nonce'
ATTRIBUTES = 'attributes'
VERIFIABLE_ATTRIBUTES = 'verifiableAttributes'
PREDICATES = 'predicates'
TXN_TIME = 'txnTime'
TXN_DATA = "txnData"
LAST_TXN = "lastTxn"
TXNS = "Txns"
BY = "by"
FORCE = 'force'

AUDIT_TXN_VIEW_NO = "viewNo"
AUDIT_TXN_PP_SEQ_NO = "ppSeqNo"
AUDIT_TXN_LEDGERS_SIZE = "ledgerSize"
AUDIT_TXN_LEDGER_ROOT = "ledgerRoot"
AUDIT_TXN_STATE_ROOT = "stateRoot"
AUDIT_TXN_PRIMARIES = "primaries"


# State proof fields
STATE_PROOF = 'state_proof'
ROOT_HASH = "root_hash"
MULTI_SIGNATURE = "multi_signature"
PROOF_NODES = "proof_nodes"
VALUE = 'value'

MULTI_SIGNATURE_SIGNATURE = 'signature'
MULTI_SIGNATURE_PARTICIPANTS = 'participants'
MULTI_SIGNATURE_VALUE = 'value'
MULTI_SIGNATURE_VALUE_LEDGER_ID = 'ledger_id'
MULTI_SIGNATURE_VALUE_STATE_ROOT = 'state_root_hash'
MULTI_SIGNATURE_VALUE_TXN_ROOT = 'txn_root_hash'
MULTI_SIGNATURE_VALUE_POOL_STATE_ROOT = 'pool_state_root_hash'
MULTI_SIGNATURE_VALUE_TIMESTAMP = 'timestamp'

# ROLES
IDENTITY_OWNER = Roles.IDENTITY_OWNER.value
STEWARD = Roles.STEWARD.value
TRUSTEE = Roles.TRUSTEE.value

IDENTITY_OWNER_STRING = None
STEWARD_STRING = 'STEWARD'
TRUSTEE_STRING = 'TRUSTEE'

# TXNs
NODE = PlenumTransactions.NODE.value
NYM = PlenumTransactions.NYM.value
GET_TXN = PlenumTransactions.GET_TXN.value
TXN_AUTHOR_AGREEMENT = PlenumTransactions.TXN_AUTHOR_AGREEMENT.value
TXN_AUTHOR_AGREEMENT_AML = PlenumTransactions.TXN_AUTHOR_AGREEMENT_AML.value
GET_TXN_AUTHOR_AGREEMENT = PlenumTransactions.GET_TXN_AUTHOR_AGREEMENT.value
GET_TXN_AUTHOR_AGREEMENT_AML = PlenumTransactions.GET_TXN_AUTHOR_AGREEMENT_AML.value

# TXN
# TODO: manye of these constants will be replaced
# by constants from Request after Request refactoring
TXN_PAYLOAD = "txn"
TXN_PAYLOAD_TYPE = "type"
TXN_PAYLOAD_PROTOCOL_VERSION = "protocolVersion"
TXN_PAYLOAD_DATA = "data"
TXN_PAYLOAD_METADATA = "metadata"
TXN_PAYLOAD_METADATA_FROM = "from"
TXN_PAYLOAD_METADATA_REQ_ID = "reqId"
TXN_PAYLOAD_METADATA_DIGEST = "digest"
TXN_PAYLOAD_METADATA_PAYLOAD_DIGEST = "payloadDigest"
TXN_PAYLOAD_METADATA_TAA_ACCEPTANCE = "taaAcceptance"
TXN_METADATA = "txnMetadata"
TXN_METADATA_TIME = "txnTime"
TXN_METADATA_ID = "txnId"
TXN_METADATA_SEQ_NO = "seqNo"
TXN_SIGNATURE = "reqSignature"
TXN_VERSION = "ver"
TXN_SIGNATURE_TYPE = "type"
ED25519 = "ED25519"
TXN_SIGNATURE_VALUES = "values"
TXN_SIGNATURE_FROM = "from"
TXN_SIGNATURE_VALUE = "value"

TXN_AUTHOR_AGREEMENT_TEXT = "text"
TXN_AUTHOR_AGREEMENT_VERSION = "version"


class ClientBootStrategy(IntEnum):
    Simple = 1
    PoolTxn = 2
    Custom = 3


class StorageType(IntEnum):
    File = 1
    Ledger = 2


class KeyValueStorageType(IntEnum):
    Leveldb = 1
    Memory = 2
    Rocksdb = 3
    ChunkedBinaryFile = 4
    BinaryFile = 5


class PreVCStrategies(IntEnum):
    VC_START_MSG_STRATEGY = 1


@unique
class LedgerState(IntEnum):
    not_synced = 1  # Still gathering consistency proofs
    syncing = 2  # Got sufficient consistency proofs, will be sending catchup
    # requests and waiting for their replies
    synced = 3  # Got replies for all catchup requests, indicating catchup
    # complete for the ledger


OP_FIELD_NAME = "op"

CLIENT_STACK_SUFFIX = "C"
CLIENT_BLACKLISTER_SUFFIX = "BLC"

NODE_BLACKLISTER_SUFFIX = "BLN"
NODE_PRIMARY_STORAGE_SUFFIX = "PS"
NODE_TXN_STORE_SUFFIX = "TS"
NODE_HASH_STORE_SUFFIX = "HS"

HS_FILE = "file"
HS_MEMORY = "memory"
HS_LEVELDB = 'leveldb'
HS_ROCKSDB = 'rocksdb'

LAST_SENT_PRE_PREPARE = 'lastSentPrePrepare'

PLUGIN_BASE_DIR_PATH = "PluginBaseDirPath"
POOL_LEDGER_ID = 0
DOMAIN_LEDGER_ID = 1
CONFIG_LEDGER_ID = 2
AUDIT_LEDGER_ID = 3

# Store labels
BLS_LABEL = 'bls'
TS_LABEL = 'ts'
IDR_CACHE_LABEL = 'idr'
ATTRIB_LABEL = 'attrib'

VALID_LEDGER_IDS = (POOL_LEDGER_ID, DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID, AUDIT_LEDGER_ID)

CURRENT_PROTOCOL_VERSION = PlenumProtocolVersion.TXN_FORMAT_1_0_SUPPORT.value

OPERATION_SCHEMA_IS_STRICT = False
SCHEMA_IS_STRICT = False


class NodeHooks(UniqueSet):
    PRE_STATIC_VALIDATION = 1
    POST_STATIC_VALIDATION = 2
    PRE_SIG_VERIFICATION = 3
    POST_SIG_VERIFICATION = 4
    PRE_DYNAMIC_VALIDATION = 5
    POST_DYNAMIC_VALIDATION = 6
    PRE_REQUEST_APPLICATION = 7
    POST_REQUEST_APPLICATION = 8
    PRE_REQUEST_COMMIT = 9
    POST_REQUEST_COMMIT = 10
    PRE_SEND_REPLY = 11
    POST_SEND_REPLY = 12
    POST_BATCH_CREATED = 13
    POST_BATCH_REJECTED = 14
    PRE_BATCH_COMMITTED = 15
    POST_BATCH_COMMITTED = 16
    POST_NODE_STOPPED = 17


class ReplicaHooks(UniqueSet):
    CREATE_PPR = 1
    CREATE_PR = 2
    CREATE_CM = 3
    CREATE_ORD = 4
    APPLY_PPR = 5
    VALIDATE_PR = 6
    VALIDATE_CM = 7
    BATCH_CREATED = 8
    BATCH_REJECTED = 9
