from collections import OrderedDict

# Each entry in registry is (stack name, ((host, port), verkey, pubkey))

from plenum.common.txn import ClientBootStrategy

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

baseDir = "~/.plenum/"

domainTransactionsFile = "transactions"

poolTransactionsFile = "pool_transactions"

clientBootStrategy = ClientBootStrategy.PoolTxn

hashStore = {
    "type": "file"
}

primaryStorage = None

secondaryStorage = None

OrientDB = {
    "user": "root",
    "password": "password",
    "host": "127.0.0.1",
    "port": 2424,
    "startScript": "/opt/orientdb/bin/server.sh",
    "shutdownScript": "/opt/orientdb/bin/shutdown.sh"
}

stewardThreshold = 20

# Monitoring configuration
DELTA = 0.8
LAMBDA = 60
OMEGA = 5
SendMonitorStats = True
ThroughputWindowSize = 30
DashboardUpdateFreq = 5
ThroughputGraphDuration = 240
LatencyWindowSize = 30
LatencyGraphDuration = 240
