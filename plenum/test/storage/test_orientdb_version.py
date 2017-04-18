import pytest

from plenum.persistence.orientdb_store import OrientDbStore
from plenum.common.config_util import getConfig

config = getConfig()


@pytest.mark.skip(reason='OrientDB is deprecated, if you want you can run '
                         'it, install the driver and then run the test')
def testOrientDbRequiredVersion():
    orientConf = config.OrientDB
    db = OrientDbStore(user=orientConf["user"], password=orientConf["password"],
                       host=orientConf["host"], port=orientConf["port"],
                       dbName="test")
    version = db.serverVersion
    assert version and version[0] >= 2 and version[1] >= 2
