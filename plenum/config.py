from collections import OrderedDict

# Each entry in registry is (stack name, ((host, port), verkey, pubkey))

from plenum.common.txn import ClientBootStrategy

nodeReg = OrderedDict([
    ('Alpha', (('qcfbchain.cloudapp.net', 9701), '0490a246940fa636235c664b8e767f2a79e48899324c607d73241e11e558bbd7', 'ea95ae1c913b59b7470443d79a6578c1b0d6e1cad0471d10cee783dbf9fda655')),
    ('Beta', (('qcfbchain.cloudapp.net', 9703), 'b628de8ac1198031bd1dba3ab38077690ca9a65aa18aec615865578af309b3fb', '18833482f6625d9bc788310fe390d44dd268427003f9fd91534e7c382501cd3c')),
    ('Gamma', (('52.160.103.164', 9705), '92d820f5eb394cfaa8d6e462f14708ddecbd4dbe0a388fbc7b5da1d85ce1c25a', 'b7e161743144814552e90dc3e1c11d37ee5a488f9b669de9b8617c4af69d566c')),
    ('Delta', (('52.160.103.164', 9707), '3af81a541097e3e042cacbe8761c0f9e54326049e1ceda38017c95c432312f6f', '8b112025d525c47e9df81a6de2966e1b4ee1ac239766e769f19d831175a04264'))
])

cliNodeReg = OrderedDict([
    ('AlphaC', (('qcfbchain.cloudapp.net', 9702), '0490a246940fa636235c664b8e767f2a79e48899324c607d73241e11e558bbd7', 'ea95ae1c913b59b7470443d79a6578c1b0d6e1cad0471d10cee783dbf9fda655')),
    ('BetaC', (('qcfbchain.cloudapp.net', 9704), 'b628de8ac1198031bd1dba3ab38077690ca9a65aa18aec615865578af309b3fb', '18833482f6625d9bc788310fe390d44dd268427003f9fd91534e7c382501cd3c')),
    ('GammaC', (('52.160.103.164', 9706), '92d820f5eb394cfaa8d6e462f14708ddecbd4dbe0a388fbc7b5da1d85ce1c25a', 'b7e161743144814552e90dc3e1c11d37ee5a488f9b669de9b8617c4af69d566c')),
    ('DeltaC', (('52.160.103.164', 9708), '3af81a541097e3e042cacbe8761c0f9e54326049e1ceda38017c95c432312f6f', '8b112025d525c47e9df81a6de2966e1b4ee1ac239766e769f19d831175a04264'))
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
