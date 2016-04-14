from typing import Dict

from plenum.common.util import getlogger

logger = getlogger()


class GraphStore:
    def __init__(self, store):
        self.store = store
        self.store.classesNeeded = self.classesNeeded()
        self.client = store.client
        self.bootstrap()

    def classesNeeded(self):
        raise NotImplementedError

    def bootstrap(self):
        self.store.createClasses()

    def createVertexClass(self, className: str, properties: Dict=None):
        self.client.command("create class {} extends V".format(className))
        if properties:
            self.store.createClassProperties(className, properties)

    def createEdgeClass(self, className: str, properties: Dict=None):
        self.client.command("create class {} extends E".format(className))
        if properties:
            self.store.createClassProperties(className, properties)

    def createEntity(self, createCmd, **kwargs):
        attributes = []
        if len(kwargs) > 0:
            createCmd += " set "
        for key, val in kwargs.items():
            attributes.append("{} = '{}'".format(key, val))
        createCmd += ", ".join(attributes)
        return self.client.command(createCmd)[0]

    def createVertex(self, name, **kwargs):
        cmd = "create vertex {}".format(name)
        return self.createEntity(cmd, **kwargs)

    def createEdge(self, name, frm, to, **kwargs):
        cmd = "create edge {} from {} to {}".format(name, frm, to)
        return self.createEntity(cmd, **kwargs)
