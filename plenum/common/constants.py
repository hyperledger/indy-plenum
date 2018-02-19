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

BLACKLIST = "BLACKLIST"

THREE_PC_PREFIX = "3PC: "
MONITORING_PREFIX = "MONITORING: "
VIEW_CHANGE_PREFIX = "VIEW CHANGE: "
CATCH_UP_PREFIX = "CATCH-UP: "
PRIMARY_SELECTION_PREFIX = "PRIMARY SELECTION: "
BLS_PREFIX = "BLS: "
OBSERVER_PREFIX = "OBSERVER: "

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

# State proof fields
STATE_PROOF = 'state_proof'
ROOT_HASH = "root_hash"
MULTI_SIGNATURE = "multi_signature"
PROOF_NODES = "proof_nodes"

MULTI_SIGNATURE_SIGNATURE = 'signature'
MULTI_SIGNATURE_PARTICIPANTS = 'participants'
MULTI_SIGNATURE_VALUE = 'value'
MULTI_SIGNATURE_VALUE_LEDGER_ID = 'ledger_id'
MULTI_SIGNATURE_VALUE_STATE_ROOT = 'state_root_hash'
MULTI_SIGNATURE_VALUE_TXN_ROOT = 'txn_root_hash'
MULTI_SIGNATURE_VALUE_POOL_STATE_ROOT = 'pool_state_root_hash'
MULTI_SIGNATURE_VALUE_TIMESTAMP = 'timestamp'

# ROLES
STEWARD = Roles.STEWARD.value
TRUSTEE = Roles.TRUSTEE.value

# TXNs
NODE = PlenumTransactions.NODE.value
NYM = PlenumTransactions.NYM.value
GET_TXN = PlenumTransactions.GET_TXN.value

POOL_TXN_TYPES = {NODE, }


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

PLUGIN_BASE_DIR_PATH = "PluginBaseDirPath"
POOL_LEDGER_ID = 0
DOMAIN_LEDGER_ID = 1
CONFIG_LEDGER_ID = 2

VALID_LEDGER_IDS = (POOL_LEDGER_ID, DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID)

CURRENT_PROTOCOL_VERSION = PlenumProtocolVersion.STATE_PROOF_SUPPORT.value


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


class ReplicaHooks(UniqueSet):
    CREATE_PPR = 1
    CREATE_PR = 2
    CREATE_CM = 3
    CREATE_ORD = 4
    RECV_PPR = 5
    RECV_PR = 6
    RECV_CM = 7
