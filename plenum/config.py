from collections import OrderedDict

# Each entry in registry is (stack name, ((host, port), verkey, pubkey))

from plenum.common.txn import ClientBootStrategy

nodeReg = OrderedDict([
    ('Alpha', ('qcfbchain.cloudapp.net', 9701)),
    ('Beta', ('qcfbchain.cloudapp.net', 9703)),
    ('Gamma', ('52.160.103.164', 9705)),
    ('Delta', ('52.160.103.164', 9707))
])

cliNodeReg = OrderedDict([
    ('AlphaC', ('qcfbchain.cloudapp.net', 9702)),
    ('BetaC', ('qcfbchain.cloudapp.net', 9704)),
    ('GammaC', ('52.160.103.164', 9706)),
    ('DeltaC', ('52.160.103.164', 9708))
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

sendMonitorStats = False