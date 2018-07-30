import os
import sys
from collections import OrderedDict

import logging

from plenum.common.constants import ClientBootStrategy, HS_FILE, HS_LEVELDB, \
    HS_ROCKSDB, HS_MEMORY, KeyValueStorageType
from plenum.common.types import PLUGIN_TYPE_STATS_CONSUMER

walletsDir = 'wallets'
clientDataDir = 'data/clients'
GENERAL_CONFIG_DIR = '/etc/indy'
# walletDir = 'wallet'

# it should be filled from baseConfig
NETWORK_NAME = ''
USER_CONFIG_DIR = None

GENERAL_CONFIG_FILE = 'indy_config.py'
NETWORK_CONFIG_FILE = 'indy_config.py'
USER_CONFIG_FILE = 'indy_config.py'

pool_transactions_file_base = 'pool_transactions'
domain_transactions_file_base = 'domain_transactions'
config_transactions_file_base = 'config_transactions'
genesis_file_suffix = '_genesis'

poolTransactionsFile = pool_transactions_file_base
domainTransactionsFile = domain_transactions_file_base
configTransactionsFile = config_transactions_file_base

stateTsStorage = KeyValueStorageType.Rocksdb

poolStateDbName = 'pool_state'
domainStateDbName = 'domain_state'
configStateDbName = 'config_state'
stateTsDbName = "state_ts_db"

stateSignatureDbName = 'state_signature'

# There is only one seqNoDB as it maintain the mapping of
# request id to sequence numbers
seqNoDbName = 'seq_no_db'

clientBootStrategy = ClientBootStrategy.PoolTxn

hashStore = {
    "type": HS_ROCKSDB
}

primaryStorage = None

domainStateStorage = KeyValueStorageType.Rocksdb
poolStateStorage = KeyValueStorageType.Rocksdb
configStateStorage = KeyValueStorageType.Rocksdb
reqIdToTxnStorage = KeyValueStorageType.Rocksdb

stateSignatureStorage = KeyValueStorageType.Rocksdb

transactionLogDefaultStorage = KeyValueStorageType.Rocksdb

rocksdb_default_config = {
    'max_open_files': None,
    'max_log_file_size': None,
    'keep_log_file_num': 5,
    # Compaction related options
    'target_file_size_base': None,
    # Memtable related options
    'write_buffer_size': None,
    'max_write_buffer_number': None,
    'block_cache_size': None,
    'block_cache_compressed_size': None,
    'no_block_cache': None,
    'block_size': None,
    'db_log_dir': None
}

rocksdb_merkle_leaves_config = rocksdb_default_config.copy()
# Change merkle leaves config here if you fully understand what's going on

rocksdb_merkle_nodes_config = rocksdb_default_config.copy()
# Change nodes config here if you fully understand what's going on

rocksdb_state_config = rocksdb_default_config.copy()
# Change state config here if you fully understand what's going on

rocksdb_transactions_config = rocksdb_default_config.copy()
# Change transactions config here if you fully understand what's going on

rocksdb_seq_no_db_config = rocksdb_default_config.copy()
# Change seq_no_db config here if you fully understand what's going on

rocksdb_state_signature_config = rocksdb_default_config.copy()
# Change state_signature config here if you fully understand what's going on

rocksdb_state_ts_db_config = rocksdb_default_config.copy()
# Change state_ts_db config here if you fully understand what's going on

# FIXME: much more clear solution is to check which key-value storage type is
# used for each storage and set corresponding config, but for now only RocksDB
# tuning is supported (now other storage implementations ignore this parameter)
# so here we set RocksDB configs unconditionally for simplicity.
db_merkle_leaves_config = rocksdb_merkle_leaves_config
db_merkle_nodes_config = rocksdb_merkle_nodes_config
db_state_config = rocksdb_state_config
db_transactions_config = rocksdb_transactions_config
db_seq_no_db_config = rocksdb_seq_no_db_config
db_state_signature_config = rocksdb_state_signature_config
db_state_ts_db_config = rocksdb_state_ts_db_config


DefaultPluginPath = {
    # PLUGIN_BASE_DIR_PATH: "<abs path of plugin directory can be given here,
    #  if not given, by default it will pickup plenum/server/plugin path>",
    PLUGIN_TYPE_STATS_CONSUMER: "stats_consumer"
}

PluginsDir = "plugins"

stewardThreshold = 20

# Monitoring configuration
PerfCheckFreq = 10
UnorderedCheckFreq = 60
ForceViewChangeFreq = 0

# Temporarily reducing DELTA till the calculations for extra work are not
# incorporated
DELTA = 0.1
LAMBDA = 240
OMEGA = 20
SendMonitorStats = False
ThroughputWindowSize = 30
DashboardUpdateFreq = 5
ThroughputGraphDuration = 240
LatencyWindowSize = 30
LatencyGraphDuration = 240

# This parameter defines minimal count of accumulated latencies for each client
MIN_LATENCY_COUNT = 10

# Two following parameters define collecting statistic timeout for
# collecting ordered request and throughput evaluating them.
# In other words, during ThroughputInnerWindowSize * ThroughputMinActivityThreshold seconds,
# throughput will returned as None for corresponding getThroughput methods.
ThroughputInnerWindowSize = 15
ThroughputMinActivityThreshold = 16

notifierEventTriggeringConfig = {
    'clusterThroughputSpike': {
        'bounds_coeff': 10,
        'min_cnt': 15,
        'freq': 60,
        'min_activity_threshold': 10,
        'use_weighted_bounds_coeff': True,
        'enabled': True
    },
    'nodeRequestSpike': {
        'bounds_coeff': 10,
        'min_cnt': 15,
        'freq': 60,
        'min_activity_threshold': 10,
        'use_weighted_bounds_coeff': True,
        'enabled': True
    }
}

SpikeEventsEnabled = False

# Stats server configuration
STATS_SERVER_IP = '127.0.0.1'
STATS_SERVER_PORT = 30000
STATS_SERVER_MESSAGE_BUFFER_MAX_SIZE = 1000

# Node status configuration
DUMP_VALIDATOR_INFO_INIT_SEC = 3
DUMP_VALIDATOR_INFO_PERIOD_SEC = 60

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

# Timeout factor after which a node starts requesting ledgerStatus if has
# not found enough matching
LedgerStatusTimeout = 5

# Timeout factor after which a node starts requesting transactions
# We assume, that making consistency proof + iterate over all transactions (getAllTxn)
# will take a little time (0.003 sec for making cp for 10 000 txns +
#                          0.2 sec for getAllTxn for 10 000 txn)
# Therefore, node communication is the most cost operation
# Timeout for pool catchuping would be nodeCount * CatchupTransactionsTimeout
CatchupTransactionsTimeout = 6

# Log configuration
logRotationBackupCount = 150
logRotationMaxBytes = 100 * 1024 * 1024
logRotationCompression = "xz"
logFormat = '{asctime:s}|{levelname:s}|{filename:s}|{message:s}'
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

# Number of messages zstack accepts at once
LISTENER_MESSAGE_QUOTA = 100
REMOTES_MESSAGE_QUOTA = 100

# After `Max3PCBatchSize` requests or `Max3PCBatchWait`, whichever is earlier,
# a 3 phase batch is sent
# Max batch size for 3 phase commit
Max3PCBatchSize = 10000
# Max time to wait before creating a batch for 3 phase commit
Max3PCBatchWait = 1

# Each node keeps a map of PrePrepare sequence numbers and the corresponding
# txn seqnos that came out of it. Helps in servicing Consistency Proof Requests
ProcessedBatchMapsToKeep = 1000

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

# Connections tracking and stack restart parameters.
# NOTE: TRACK_CONNECTED_CLIENTS_NUM_ENABLED must be set to True
# if CLIENT_STACK_RESTART_ENABLED is set to True as stack restart
# mechanism uses clients connections tracking.
TRACK_CONNECTED_CLIENTS_NUM_ENABLED = True
CLIENT_STACK_RESTART_ENABLED = True
MAX_CONNECTED_CLIENTS_NUM = 10000
MIN_STACK_RESTART_TIMEOUT = 1800  # seconds
STACK_POSTRESTART_WAIT_TIME = 2  # seconds
MAX_STACK_RESTART_TIME_DEVIATION = 300  # seconds

VIEW_CHANGE_TIMEOUT = 600  # seconds
INITIAL_PROPOSE_VIEW_CHANGE_TIMEOUT = 60
INSTANCE_CHANGE_TIMEOUT = 60
MAX_CATCHUPS_DONE_DURING_VIEW_CHANGE = 5
MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE = 300

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
ENC_FIELD_LIMIT = 5 * 1024
RAW_FIELD_LIMIT = 5 * 1024
SIGNATURE_TYPE_FIELD_LIMIT = 16
BLS_KEY_LIMIT = 512
BLS_SIG_LIMIT = 512
BLS_MULTI_SIG_LIMIT = 512
VERSION_FIELD_LIMIT = 128
DATETIME_LIMIT = 35

PLUGIN_ROOT = 'plenum.server.plugin'
ENABLED_PLUGINS = []

# 0 for normal operation
# 1 for recorder
# 2 during replay
STACK_COMPANION = 0

ENABLE_INCONSISTENCY_WATCHER_NETWORK = False

# None or 'kv'
METRICS_COLLECTOR_TYPE = None
METRICS_KV_STORAGE = KeyValueStorageType.Rocksdb
METRICS_KV_DB_NAME = 'metrics_db'
METRICS_KV_CONFIG = rocksdb_default_config.copy()
