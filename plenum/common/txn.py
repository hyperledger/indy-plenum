# TODO: Change this file name to `constants`

# inter-node communication
from enum import IntEnum

NOMINATE = "NOMINATE"
REELECTION = "REELECTION"
PRIMARY = "PRIMARY"
PRIMDEC = "PRIMARYDECIDED"

BATCH = "BATCH"

REQACK = "REQACK"

REQNACK = "REQNACK"

PROPAGATE = "PROPAGATE"

PREPREPARE = "PREPREPARE"
PREPARE = "PREPARE"
COMMIT = "COMMIT"
REPLY = "REPLY"

ORDERED = "ORDERED"
REQDIGEST = "REQDIGEST"

INSTANCE_CHANGE = "INSTANCE_CHANGE"

BLACKLIST = "BLACKLIST"


TXN_TYPE = "type"
TXN_ID = "txnId"
ORIGIN = "origin"
TARGET_NYM = "dest"
DATA = "data"
ALIAS = "alias"
PUBKEY = "pubkey"
NODE_IP = "node_ip"
NODE_PORT = "node_port"
CLIENT_IP = "client_ip"
CLIENT_PORT = "client_port"
NEW_NODE = "NEW_NODE"
NEW_STEWARD = "NEW_STEWARD"
NEW_CLIENT = "NEW_CLIENT"
STEWARD = "STEWARD"
CLIENT = "CLIENT"
ROLE = 'role'
NONCE = 'nonce'
ATTRIBUTES = 'attributes'
TXN_TIME = 'txnTime'
TXN_DATA = "txnData"

LAST_TXN = "lastTxn"
TXNS = "Txns"


class ClientBootStrategy(IntEnum):
    Simple = 1
    PoolTxn = 2
    Custom = 3


class StorageType(IntEnum):
    File = 1
    Ledger = 2
    OrientDB = 3
