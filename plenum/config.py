
import os
import sys
from collections import OrderedDict

import logging

from plenum.common.constants import ClientBootStrategy, HS_FILE, KeyValueStorageType
from plenum.common.types import PLUGIN_TYPE_STATS_CONSUMER

# Each entry in registry is (stack name, ((host, port), verkey, pubkey))

nodeReg = OrderedDict([
    ('Alpha', ('127.0.0.1', 9701)),
    ('Beta', ('127.0.0.1', 9703)),
    ('Gamma', ('127.0.0.1', 9705)),
    ('Delta', ('127.0.0.1', 9707))
])

cliNodeReg = OrderedDict([
    ('AlphaC', ('127.0.0.1', 9702)),
    ('BetaC', ('127.0.0.1', 9704)),
    ('GammaC', ('127.0.0.1', 9706)),
    ('DeltaC', ('127.0.0.1', 9708))
])

baseDir = '~/.plenum/'
walletsDir = 'wallets'
NODE_BASE_DATA_DIR = baseDir
nodeDataDir = 'data/nodes'
clientDataDir = 'data/clients'
LOG_DIR = os.path.join(baseDir, "log")
GENERAL_CONFIG_DIR = '/etc/indy'
# walletDir = 'wallet'

# it should be filled from baseConfig
NETWORK_NAME = ''

GENERAL_CONFIG_FILE = 'plenum_config.py'
NETWORK_CONFIG_FILE = 'plenum_config.py'
USER_CONFIG_FILE = 'plenum_config.py'

pool_transactions_file_base = 'pool_transactions'
domain_transactions_file_base = 'domain_transactions'
genesis_file_suffix = '_genesis'

poolTransactionsFile = pool_transactions_file_base
domainTransactionsFile = domain_transactions_file_base


poolStateDbName = 'pool_state'
domainStateDbName = 'domain_state'

stateSignatureDbName = 'state_signature'

# There is only one seqNoDB as it maintain the mapping of
# request id to sequence numbers
seqNoDbName = 'seq_no_db'

clientBootStrategy = ClientBootStrategy.PoolTxn

hashStore = {
    "type": HS_FILE
}

primaryStorage = None

domainStateStorage = KeyValueStorageType.Leveldb
poolStateStorage = KeyValueStorageType.Leveldb
reqIdToTxnStorage = KeyValueStorageType.Leveldb

stateSignatureStorage = KeyValueStorageType.Leveldb

DefaultPluginPath = {
    # PLUGIN_BASE_DIR_PATH: "<abs path of plugin directory can be given here,
    #  if not given, by default it will pickup plenum/server/plugin path>",
    PLUGIN_TYPE_STATS_CONSUMER: "stats_consumer"
}

PluginsDir = "plugins"

stewardThreshold = 20

# Monitoring configuration
PerfCheckFreq = 10

# Temporarily reducing DELTA till the calculations for extra work are not
# incorporated
DELTA = 0.4
LAMBDA = 60
OMEGA = 5
SendMonitorStats = False
ThroughputWindowSize = 30
DashboardUpdateFreq = 5
ThroughputGraphDuration = 240
LatencyWindowSize = 30
LatencyGraphDuration = 240
notifierEventTriggeringConfig = {
    'clusterThroughputSpike': {
        'coefficient': 3,
        'minCnt': 100,
        'freq': 60,
        'minActivityThreshold': 2,
        'enabled': True
    },
    'nodeRequestSpike': {
        'coefficient': 3,
        'minCnt': 100,
        'freq': 60,
        'minActivityThreshold': 2,
        'enabled': True
    }
}

SpikeEventsEnabled = False

# Stats server configuration
STATS_SERVER_IP = '127.0.0.1'
STATS_SERVER_PORT = 30000
STATS_SERVER_MESSAGE_BUFFER_MAX_SIZE = 1000

# Node status configuration
DUMP_VALIDATOR_INFO_PERIOD_SEC = 60

RAETLogLevel = "terse"
RAETLogLevelCli = "mute"
RAETLogFilePath = os.path.join(os.path.expanduser(baseDir), "raet.log")
RAETLogFilePathCli = None
RAETMessageTimeout = 60

# Controls sending of view change messages, a node will only send view change
# messages if it did not send any sent instance change messages in last
# `ViewChangeWindowSize` seconds
ViewChangeWindowSize = 60

# A node if finds itself disconnected from primary of the master instance will
# wait for `ToleratePrimaryDisconnection` before sending a view change message
ToleratePrimaryDisconnection = 2

# Timeout factor after which a node starts requesting consistency proofs if has
# not found enough matching
ConsistencyProofsTimeout = 5

# Timeout factor after which a node starts requesting transactions
CatchupTransactionsTimeout = 5


# Log configuration
logRotationWhen = 'D'
logRotationInterval = 1
logRotationBackupCount = 10
logRotationMaxBytes = 100 * 1024 * 1024
logFormat = '{asctime:s} | {levelname:8s} | {filename:20s} ({lineno: >4}) | {funcName:s} | {message:s}'
logFormatStyle = '{'
logLevel = logging.NOTSET
enableStdOutLogging = True

# OPTIONS RELATED TO TESTS

# TODO test 60sec
TestRunningTimeLimitSec = 100

# Expected time for one stack to get connected to another
ExpectedConnectTime = 3.3 if sys.platform == 'win32' else 2

# Since the ledger is stored in a flat file, this makes the ledger do
# an fsync on every write. Making it True can significantly slow
# down writes as shown in a test `test_file_store_perf.py` in the ledger
# repository
EnsureLedgerDurability = False

log_override_tags = dict(cli={}, demo={})

# TODO needs to be refactored to use a transport protocol abstraction
UseZStack = True


# Number of messages zstack accepts at once
LISTENER_MESSAGE_QUOTA = 100
REMOTES_MESSAGE_QUOTA = 100

# After `Max3PCBatchSize` requests or `Max3PCBatchWait`, whichever is earlier,
# a 3 phase batch is sent
# Max batch size for 3 phase commit
Max3PCBatchSize = 100
# Max time to wait before creating a batch for 3 phase commit
Max3PCBatchWait = .001


# Each node keeps a map of PrePrepare sequence numbers and the corresponding
# txn seqnos that came out of it. Helps in servicing Consistency Proof Requests
ProcessedBatchMapsToKeep = 100


# After `MaxStateProofSize` requests or `MaxStateProofSize`, whichever is
# earlier, a signed state proof is sent
# Max 3 state proof size
MaxStateProofSize = 10
# State proof timeout
MaxStateProofTime = 3


# After ordering every `CHK_FREQ` batches, replica sends a CHECKPOINT
CHK_FREQ = 100

# Difference between low water mark and high water mark
LOG_SIZE = 3 * CHK_FREQ


CLIENT_REQACK_TIMEOUT = 5
CLIENT_REPLY_TIMEOUT = 15
CLIENT_MAX_RETRY_ACK = 5
CLIENT_MAX_RETRY_REPLY = 5

VIEW_CHANGE_TIMEOUT = 60  # seconds
MAX_CATCHUPS_DONE_DURING_VIEW_CHANGE = 5
MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE = 15

# permissions for keyring dirs/files
WALLET_DIR_MODE = 0o700  # drwx------
WALLET_FILE_MODE = 0o600  # -rw-------

# This timeout is high enough so that even if some PRE-PREPAREs are stashed
# because of being delivered out of order or being out of watermarks or not
# having finalised requests.
ACCEPTABLE_DEVIATION_PREPREPARE_SECS = 600  # seconds

# TXN fields length limits
ALIAS_FIELD_LIMIT = 256
DIGEST_FIELD_LIMIT = 512
TIE_IDR_FIELD_LIMIT = 256
NAME_FIELD_LIMIT = 256
SENDER_CLIENT_FIELD_LIMIT = 256
HASH_FIELD_LIMIT = 256
SIGNATURE_FIELD_LIMIT = 512
JSON_FIELD_LIMIT = 5 * 1024
DATA_FIELD_LIMIT = 5 * 1024
NONCE_FIELD_LIMIT = 512
ORIGIN_FIELD_LIMIT = 128
ENC_FIELD_LIMIT = 16
RAW_FIELD_LIMIT = 5 * 1024
SIGNATURE_TYPE_FIELD_LIMIT = 16
BLS_KEY_LIMIT = 512
BLS_SIG_LIMIT = 512
BLS_MULTI_SIG_LIMIT = 512
VERSION_FIELD_LIMIT = 128
