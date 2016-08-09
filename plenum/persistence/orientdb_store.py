from typing import Dict

import pyorient
from plenum.common.util import getlogger, error

logger = getlogger()


class OrientDbStore:
    def __init__(self, user, password, dbName, host="localhost", port=2424,
                 dbType=pyorient.DB_TYPE_GRAPH,
                 storageType=pyorient.STORAGE_TYPE_MEMORY):
        self.dbType = dbType
        try:
            self.client = pyorient.OrientDB(host=host, port=port)
            self.session_id = self.client.connect(user, password)
        except pyorient.exceptions.PyOrientConnectionException:
            error("OrientDB connection failed. Check if DB is running "
                  "on port {}".format(port))
        if not self.client.db_exists(dbName, storageType):
            self.createDb(dbName, dbType, storageType)
        self.client.db_open(dbName, user, password)
        if not (self.serverVersion and self.serverVersion[0] >= 2 and
                        self.serverVersion[1] >= 2):
            error("OrientDB version should be atleast 2.2. Current version is {}"
                  .format(".".join(self.serverVersion)))

    @property
    def serverVersion(self):
        if self.client and self.client.version:
            version = self.client.version
            return str(version.major), str(version.minor), str(version.build)

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
