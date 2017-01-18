from typing import Dict

import pyorient

from plenum.common.log import getlogger
from plenum.persistence.graph_store import GraphStore
from plenum.persistence.orientdb_store import OrientDbStore

logger = getlogger()


class OrientDbGraphStore(GraphStore):
    def __init__(self, store: OrientDbStore):
        assert store.dbType == pyorient.DB_TYPE_GRAPH, \
            "OrientDbGraphStore must be initialized with dbType=DB_TYPE_GRAPH"
        super().__init__(store)

    @property
    def classesNeeded(self):
        raise NotImplementedError

    def bootstrap(self):
        self.store.createClasses(self.classesNeeded)

    def createVertexClass(self, className: str, properties: Dict=None):
        self.createClass(className, "V", properties)

    def createEdgeClass(self, className: str, properties: Dict=None):
        self.createClass(className, "E", properties)

    def createClass(self, className: str, superclass: str, properties: Dict=None):
        self.client.command("create class {} extends {}".
                            format(className, superclass))
        # TODO tried the following to see if it increases performance, but
        # it didn't seem to.
        # See https://www.mail-archive.com/orient-database@googlegroups.com/msg12419.html
        # self.client.command("create class {}".format(className))
        # self.client.command("alter class {} superclass {}".
        #                     format(className, superclass))
        if properties:
            self.store.createClassProperties(className, properties)

    def addEdgeConstraint(self, edgeClass, iN=None, out=None):
        if iN:
            self.client.command("create property {}.in link {}".
                                format(edgeClass, iN))
        if out:
            self.client.command("create property {}.out link {}".
                                format(edgeClass, out))

    def createVertex(self, vertexName, **kwargs):
        cmd = "create vertex {}".format(vertexName)
        return self._createEntity(cmd, **kwargs)

    def createEdge(self, edgeName, edgeFrm, edgeTo, **kwargs):
        cmd = "create edge {} from {} to {}".format(edgeName, edgeFrm, edgeTo)
        return self._createEntity(cmd, **kwargs)

    def _createEntity(self, createCmd, **kwargs):
        if len(kwargs) > 0:
            createCmd += " set "
        createCmd += self.store.getPlaceHolderQueryStringFromDict(kwargs)
        return self.client.command(createCmd)[0]

    def getEntityByUniqueAttr(self, entityClassName, attrName, attrValue):
        query = "select from {} where {} = " + \
                ("{}" if isinstance(attrValue, (int, float)) else "'{}'")
        query = query.format(entityClassName, attrName, attrValue)
        result = self.client.command(query)
        return None if not result else result[0]

    def getEntityByAttrs(self, entityClassName, attrs: Dict):
        attrStr = self.store.getPlaceHolderQueryStringFromDict(attrs,
                                                               joiner=" and ")
        query = "select from {} where {}".format(entityClassName, attrStr)
        result = self.client.command(query)
        return None if not result else result[0]

    def countEntitiesByAttrs(self, entityClassName, attrs: Dict):
        attrStr = self.store.getPlaceHolderQueryStringFromDict(attrs,
                                                               joiner=" and ")
        result = self.client.command("select count(*) from {} where {}".
                                     format(entityClassName, attrStr))
        return result[0].oRecordData['count']

    def updateEntityWithUniqueId(self, entityClassName, uniqueIdKey,
                                 uniqueIdVal, **kwargs):
        if len(kwargs) > 0:
            cmd = "update {} set {} where {}".format(
                entityClassName,
                self.store.getPlaceHolderQueryStringFromDict(kwargs),
                self.store.getPlaceHolderQueryStringFromDict({
                    uniqueIdKey: uniqueIdVal}))
            self.client.command(cmd)