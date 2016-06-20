from typing import Dict

import pyorient

from plenum.common.util import getlogger
from plenum.persistence.graph_store import GraphStore
from plenum.persistence.orientdb_store import OrientDbStore

logger = getlogger()


class OrientDbGraphStore(GraphStore):
    def __init__(self, store: OrientDbStore):
        assert store.dbType == pyorient.DB_TYPE_GRAPH, \
            "OrientDbGraphStore must be initialized with dbType=DB_TYPE_GRAPH"
        super().__init__(store)

    def classesNeeded(self):
        raise NotImplementedError

    def bootstrap(self):
        self.store.createClasses(self.classesNeeded())

    def createVertexClass(self, className: str, properties: Dict=None):
        self.client.command("create class {} extends V".format(className))
        if properties:
            self.store.createClassProperties(className, properties)

    def createEdgeClass(self, className: str, properties: Dict=None):
        self.client.command("create class {} extends E".format(className))
        if properties:
            self.store.createClassProperties(className, properties)

    def addEdgeConstraint(self, edgeClass, iN=None, out=None):
        if iN:
            self.client.command("create property {}.in link {}".
                                format(edgeClass, iN))
        if out:
            self.client.command("create property {}.out link {}".
                                format(edgeClass, out))

    def createVertex(self, name, **kwargs):
        cmd = "create vertex {}".format(name)
        return self._createEntity(cmd, **kwargs)

    def createEdge(self, edgeName, edgeFrm, edgeTo, **kwargs):
        cmd = "create edge {} from {} to {}".format(edgeName, edgeFrm, edgeTo)
        return self._createEntity(cmd, **kwargs)

    def _createEntity(self, createCmd, **kwargs):
        attributes = []
        if len(kwargs) > 0:
            createCmd += " set "
        for key, val in kwargs.items():
            valPlaceHolder = "{}" if isinstance(val, int) else "'{}'"
            attributes.append(("{} = "+valPlaceHolder).format(key, val))
        createCmd += ", ".join(attributes)
        return self.client.command(createCmd)[0]
