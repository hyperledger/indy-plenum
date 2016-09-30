from plenum.persistence.orientdb_store import OrientDbStore
from plenum.common.util import getConfig

config = getConfig()


def testOrientDbRequiredVersion():
    orientConf = config.OrientDB
    db = OrientDbStore(user=orientConf["user"], password=orientConf["password"],
                       host=orientConf["host"], port=orientConf["port"],
                       dbName="test")
    version = db.serverVersion
    assert version and version[0] >= 2 and version[1] >= 2
