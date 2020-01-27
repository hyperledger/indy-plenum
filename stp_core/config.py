import os

import sys

import logging

baseDir = os.getcwd()

# Log configuration
logRotationBackupCount = 150
logRotationMaxBytes = 100 * 1024 * 1024
logRotationCompression = "xz"
logFormat = '{asctime:s}|{levelname:s}|{filename:s}|{message:s}'
logFormatStyle = '{'

logLevel = logging.NOTSET
enableStdOutLogging = True

RETRY_TIMEOUT_NOT_RESTRICTED = 2
RETRY_TIMEOUT_RESTRICTED = 2
MAX_RECONNECT_RETRY_ON_SAME_SOCKET = 3

# Enables/disables debug mode for Looper class
LOOPER_DEBUG = False

# All messages exceeding the limit will be rejected without processing
MSG_LEN_LIMIT = 128 * 1024

# Quotas configuration
ENABLE_DYNAMIC_QUOTAS = False
MAX_REQUEST_QUEUE_SIZE = 1000
NODE_TO_NODE_STACK_QUOTA = 1000
CLIENT_TO_NODE_STACK_QUOTA = 100
NODE_TO_NODE_STACK_SIZE = MSG_LEN_LIMIT * 50
CLIENT_TO_NODE_STACK_SIZE = 1024 * 1024

# Zeromq configuration
DEFAULT_LISTENER_SIZE = 20 * 1024
DEFAULT_LISTENER_QUOTA = 100
DEFAULT_SENDER_QUOTA = 100
KEEPALIVE_INTVL = 1  # seconds
KEEPALIVE_IDLE = 20  # seconds
KEEPALIVE_CNT = 10
MAX_SOCKETS = 16384 if sys.platform != 'win32' else None
ROUTER_HANDOVER = 1
ENABLE_HEARTBEATS = False
HEARTBEAT_FREQ = 5  # seconds
ZMQ_CLIENT_QUEUE_SIZE = 100  # messages (0 - no limit)
ZMQ_NODE_QUEUE_SIZE = 20000  # messages (0 - no limit)
ZMQ_STASH_TO_NOT_CONNECTED_QUEUE_SIZE = 10000
ZMQ_STASH_UNKNOWN_REMOTE_MSGS_QUEUE_SIZE = 10000
PENDING_CLIENT_LIMIT = 100
PENDING_MESSAGES_FOR_ONE_CLIENT_LIMIT = 100
RESEND_CLIENT_MSG_TIMEOUT = 30
REMOVE_CLIENT_MSG_TIMEOUT = 60 * 5
PINGS_BEFORE_SOCKET_RECONNECTION = 3
PING_RECONNECT_ENABLED = False

MAX_WAIT_FOR_BIND_SUCCESS = 120  # seconds

RETRY_CONNECT = False
RETRY_SOCKET_RECONNECT = False
NEW_CTXT_INSTANCE = False
