from typing import Dict

try:
    import pyorient
    from pyorient import DB_TYPE_GRAPH, STORAGE_TYPE_MEMORY
except ImportError:
    pyorient = None
    DB_TYPE_GRAPH = None
    STORAGE_TYPE_MEMORY = None
    print('"pyorient" is not installed. Its not required to use this project '
          'but if you really need it then install it and add dependency to '
          'setup.py')

from plenum.common.error import error
from plenum.common.exceptions import OrientDBNotRunning
from stp_core.common.log import getlogger

logger = getlogger()


class OrientDbStore:
    def __init__(self, user, password, dbName, host="localhost", port=2424,
                 dbType=DB_TYPE_GRAPH,
                 storageType=STORAGE_TYPE_MEMORY):
        self.dbType = dbType
        try:
            self.client = self.new_orientdb_client(host, port, user, password)
        except pyorient.exceptions.PyOrientConnectionException:
            raise OrientDBNotRunning("OrientDB connection failed. Check if DB is running "
                  "on port {}".format(port))
        if not self.client.db_exists(dbName, storageType):
            self.createDb(dbName, dbType, storageType)
        self.client.db_open(dbName, user, password)
        if not (self.serverVersion and self.serverVersion[0] >= 2 and
                        self.serverVersion[1] >= 2):
            error("OrientDB version should be atleast 2.2. Current version is {}"
                  .format(".".join(map(str, self.serverVersion))))

    @property
    def serverVersion(self):
        if self.client and self.client.version:
            version = self.client.version
            return version.major, version.minor, version.build

    def createDb(self, dbName, dbType, storageType):
        self.client.db_create(dbName, dbType, storageType)

    def createClasses(self, classesNeeded):
        for cls, clbk in classesNeeded:
            if not self.classExists(cls):
                logger.debug("Creating class {}".format(cls))
                clbk()
            else:
                logger.debug("Class {} already exists".format(cls))

    def classExists(self, name: str) -> bool:
        r = self.client.command("select from ( select expand( classes ) from "
                                "metadata:schema ) where name = '{}'".
                                format(name))
        return bool(r)

    def createClass(self, className):
        self.client.command("create class {}".format(className))

    def createClassProperties(self, className, properties: Dict):
        for prpName, typ in properties.items():
            self.client.command("create property {}.{} {}".
                                format(className, prpName, typ))

    def createIndexOnClass(self, className: str, prop, indexType=None):
        cmd = "create index {}.{}".format(className, prop)
        if indexType:
            if indexType not in ("unique",):
                raise ValueError("Unknown index type {}".format(indexType))
        else:
            indexType = "notunique"
        cmd += " {}".format(indexType)
        self.client.command(cmd)

    def createUniqueIndexOnClass(self, className, uniqueProperty):
        self.createIndexOnClass(className, uniqueProperty, "unique")

    def getByRecordIds(self, *rids):
        ridStr = ",".join(rids)
        return self.client.command("select from [{}]".format(ridStr))

    @staticmethod
    def getPlaceHolderQueryStringFromDict(args: Dict, joiner=", "):
        items = []
        for key, val in args.items():
            valPlaceHolder = "{}" if (isinstance(val, (int, float)) or val is None) else "'{}'"
            items.append(("{} = " + valPlaceHolder).format(key, val))
        return joiner.join(items)

    @staticmethod
    def new_orientdb_client(host, port, user, password):
        client = pyorient.OrientDB(host=host, port=port)
        session_id = client.connect(user, password)
        assert session_id, 'Problem with connecting to OrientDB'
        return client

    def wipe(self):
        """
        IMPORTANT: this is destructive; use at your own risk
        """
        assert self.client._connection, 'Client must be connected to the db'
        self.wipe_db(self.client, self.client._connection.db_opened)

    @staticmethod
    def wipe_db(client, dbName):
        try:
            client.db_drop(dbName)
            logger.debug("Dropped db {}".format(dbName))
        except Exception as ex:
            logger.debug("Error while dropping db {}: {}".format(dbName, ex))

    def close(self):
        if self.client._connection.connected:
            self.client.db_close(self.client._connection.db_opened)


def createOrientDbInMemStore(config, name, dbType):
    """
    Create and return an OrientDb in-memory store used for test cases.
    """
    host = "localhost"
    port = 2424
    try:
        client = pyorient.OrientDB(host=host, port=port)
        client.connect(user=config.OrientDB['user'],
                       password=config.OrientDB['password'])
    # except ValueError:
    #     client.connect(user=config.OrientDB['user'],
    #                    password=config.OrientDB['password'])
    except pyorient.exceptions.PyOrientConnectionException:
        error("OrientDB connection failed. Check if DB is running "
              "on port {}".format(port))
    try:
        if client.db_exists(name, pyorient.STORAGE_TYPE_MEMORY):
            client.db_drop(name, type=pyorient.STORAGE_TYPE_MEMORY)
    # This is to avoid a known bug in OrientDb.
    except pyorient.exceptions.PyOrientDatabaseException:
        client.db_drop(name, type=pyorient.STORAGE_TYPE_MEMORY)
    return OrientDbStore(user=config.OrientDB["user"],
                         password=config.OrientDB["password"],
                         dbName=name,
                         dbType=dbType,
                         storageType=pyorient.STORAGE_TYPE_MEMORY)
