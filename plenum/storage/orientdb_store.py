from abc import abstractmethod
from typing import Dict

import pyorient
from plenum.common.util import getlogger

logger = getlogger()


class OrientDbStore:
    def __init__(self, user, password, dbName, host="localhost", port=2424,
                 storageType=pyorient.STORAGE_TYPE_MEMORY):
        self.client = pyorient.OrientDB(host=host, port=port)
        self.session_id = self.client.connect(user, password)
        if not self.client.db_exists(dbName, storageType):
            self.createDb(dbName, storageType)
        self.client.db_open(dbName, user, password)
        self.bootstrap()

    # @abstractmethod
    # def classesNeeded(self):
    #     """
    #     """
    #
    # @abstractmethod
    # def bootstrap(self):
    #     """
    #     """
    #
    # @abstractmethod
    # def createDb(self, dbName, storageType):
    #     """
    #     """

    def createClasses(self):
        for cls, clbk in self.classesNeeded():
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

    def createClassProperties(self, className, properties: Dict):
        for prpName, typ in properties.items():
            self.client.command("create property {}.{} {}".format(className,
                                                                  prpName, typ))

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
        ridStr = ",".join(
            # [rid if rid.startswith("#") else "#" + rid for rid in rids])
            rids)
        return self.client.command("select from [{}]".format(ridStr))