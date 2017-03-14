# TODO: Change this file name to `constants`

# inter-node communication
from enum import IntEnum

from plenum.common.roles import Roles

NOMINATE = "NOMINATE"
REELECTION = "REELECTION"
PRIMARY = "PRIMARY"
PRIMDEC = "PRIMARYDECIDED"

BATCH = "BATCH"

REQACK = "REQACK"

REQNACK = "REQNACK"

POOL_LEDGER_TXNS = "POOL_LEDGER_TXNS"

PROPAGATE = "PROPAGATE"

PREPREPARE = "PREPREPARE"
PREPARE = "PREPARE"
COMMIT = "COMMIT"
CHECKPOINT = "CHECKPOINT"
CHECKPOINT_STATE = "CHECKPOINT_STATE"
THREE_PC_STATE = "THREE_PC_STATE"

REPLY = "REPLY"

ORDERED = "ORDERED"
REQDIGEST = "REQDIGEST"

INSTANCE_CHANGE = "INSTANCE_CHANGE"

LEDGER_STATUS = "LEDGER_STATUS"
CONSISTENCY_PROOF = "CONSISTENCY_PROOF"
CATCHUP_REQ = "CATCHUP_REQ"
CATCHUP_REP = "CATCHUP_REP"
CONS_PROOF_REQUEST = "CONS_PROOF_REQUEST"

BLACKLIST = "BLACKLIST"

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
NODE_IP = "node_ip"
NODE_PORT = "node_port"
CLIENT_IP = "client_ip"
CLIENT_PORT = "client_port"
NODE = "NODE"
# CHANGE_HA = "CHANGE_HA"
# CHANGE_KEYS = "CHANGE_KEYS"
SERVICES = "services"
VALIDATOR = "VALIDATOR"
CLIENT = "CLIENT"
NYM = "NYM"
ROLE = 'role'
NONCE = 'nonce'
ATTRIBUTES = 'attributes'
VERIFIABLE_ATTRIBUTES = 'verifiableAttributes'
TXN_TIME = 'txnTime'
TXN_DATA = "txnData"
LAST_TXN = "lastTxn"
TXNS = "Txns"

STEWARD = Roles.STEWARD.value
TRUST_ANCHOR = Roles.TRUST_ANCHOR.value
TRUSTEE = Roles.TRUSTEE.value
TGB = Roles.TGB.value

BY = "by"

POOL_TXN_TYPES = {NODE, }


class ClientBootStrategy(IntEnum):
    Simple = 1
    PoolTxn = 2
    Custom = 3


class StorageType(IntEnum):
    File = 1
    Ledger = 2
    OrientDB = 3
